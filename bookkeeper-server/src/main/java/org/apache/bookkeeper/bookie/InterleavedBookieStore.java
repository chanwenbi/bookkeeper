/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.Bookie.NopWriteCallback;
import org.apache.bookkeeper.bookie.GarbageCollectorThread.SafeEntryAdder;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.jmx.BKMBeanRegistry;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Bookie Store use {@link InterleavedLedgerStorage}.
 */
public class InterleavedBookieStore implements BookieStore, Runnable {

    static final Logger LOG = LoggerFactory.getLogger(InterleavedBookieStore.class);

    final File journalDirectory;
    final ServerConfiguration conf;

    final SyncThread syncThread;
    final LedgerManagerFactory ledgerManagerFactory;
    final LedgerManager ledgerManager;
    final LedgerStorage ledgerStorage;
    final Journal journal;

    final HandleFactory handles;

    static final long METAENTRY_ID_LEDGER_KEY = -0x1000;
    static final long METAENTRY_ID_FENCE_KEY = -0x2000;

    private LedgerDirsManager ledgerDirsManager;

    Map<Long, byte[]> masterKeyCache = Collections.synchronizedMap(new HashMap<Long, byte[]>());

    final StateTransistor stateTransistor;

    BKMBeanInfo jmxLedgerStorageBean;

    // Flag identify whether it is in shutting down progress
    private volatile boolean shuttingdown = false;

    //
    // Cookie Checker & Directory Management
    //
    public static void checkDirectoryStructure(File dir) throws IOException {
        if (!dir.exists()) {
            File parent = dir.getParentFile();
            File preV3versionFile = new File(dir.getParent(), BookKeeperConstants.VERSION_FILENAME);

            final AtomicBoolean oldDataExists = new AtomicBoolean(false);
            parent.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.endsWith(".txn") || name.endsWith(".idx") || name.endsWith(".log")) {
                        oldDataExists.set(true);
                    }
                    return true;
                }
            });
            if (preV3versionFile.exists() || oldDataExists.get()) {
                String err = "Directory layout version is less than 3, upgrade needed";
                LOG.error(err);
                throw new IOException(err);
            }
            if (!dir.mkdirs()) {
                String err = "Unable to create directory " + dir;
                LOG.error(err);
                throw new IOException(err);
            }
        }
    }

    /**
     * Check that the environment for the bookie is correct.
     * This means that the configuration has stayed the same as the
     * first run and the filesystem structure is up to date.
     */
    private void checkEnvironment(ZooKeeper zk) throws BookieException, IOException {
        if (zk == null) { // exists only for testing, just make sure directories are correct
            checkDirectoryStructure(journalDirectory);
            for (File dir : ledgerDirsManager.getAllLedgerDirs()) {
                checkDirectoryStructure(dir);
            }
            return;
        }
        try {
            String instanceId = Bookie.getInstanceId(conf, zk);
            boolean newEnv = false;
            Cookie masterCookie = Cookie.generateCookie(conf);
            if (null != instanceId) {
                masterCookie.setInstanceId(instanceId);
            }
            try {
                Cookie zkCookie = Cookie.readFromZooKeeper(zk, conf);
                masterCookie.verify(zkCookie);
            } catch (KeeperException.NoNodeException nne) {
                newEnv = true;
            }
            List<File> missedCookieDirs = new ArrayList<File>();
            checkDirectoryStructure(journalDirectory);

            // try to read cookie from journal directory
            try {
                Cookie journalCookie = Cookie.readFromDirectory(journalDirectory);
                journalCookie.verify(masterCookie);
            } catch (FileNotFoundException fnf) {
                missedCookieDirs.add(journalDirectory);
            }
            for (File dir : ledgerDirsManager.getAllLedgerDirs()) {
                checkDirectoryStructure(dir);
                try {
                    Cookie c = Cookie.readFromDirectory(dir);
                    c.verify(masterCookie);
                } catch (FileNotFoundException fnf) {
                    missedCookieDirs.add(dir);
                }
            }

            if (!newEnv && missedCookieDirs.size() > 0){
                LOG.error("Cookie exists in zookeeper, but not in all local directories. "
                        + " Directories missing cookie file are " + missedCookieDirs);
                throw new BookieException.InvalidCookieException();
            }
            if (newEnv) {
                if (missedCookieDirs.size() > 0) {
                    LOG.debug("Directories missing cookie file are {}", missedCookieDirs);
                    masterCookie.writeToDirectory(journalDirectory);
                    for (File dir : ledgerDirsManager.getAllLedgerDirs()) {
                        masterCookie.writeToDirectory(dir);
                    }
                }
                masterCookie.writeToZooKeeper(zk, conf);
            }
        } catch (KeeperException ke) {
            LOG.error("Couldn't access cookie in zookeeper", ke);
            throw new BookieException.InvalidCookieException(ke);
        } catch (UnknownHostException uhe) {
            LOG.error("Couldn't check cookies, networking is broken", uhe);
            throw new BookieException.InvalidCookieException(uhe);
        } catch (IOException ioe) {
            LOG.error("Error accessing cookie on disks", ioe);
            throw new BookieException.InvalidCookieException(ioe);
        } catch (InterruptedException ie) {
            LOG.error("Thread interrupted while checking cookies, exiting", ie);
            throw new BookieException.InvalidCookieException(ie);
        }
    }

    public LedgerDirsManager getLedgerDirsManager() {
        return ledgerDirsManager;
    }

    public static File getCurrentDirectory(File dir) {
        return new File(dir, BookKeeperConstants.CURRENT_DIR);
    }

    public static File[] getCurrentDirectories(File[] dirs) {
        File[] currentDirs = new File[dirs.length];
        for (int i = 0; i < dirs.length; i++) {
            currentDirs[i] = getCurrentDirectory(dirs[i]);
        }
        return currentDirs;
    }

    /*
     * Get the DiskFailure listener for the bookie
     */
    private LedgerDirsListener getLedgerDirsListener() {

        return new LedgerDirsListener() {

            @Override
            public void diskFull(File disk) {
                // Nothing needs to be handled here.
            }

            @Override
            public void diskFailed(File disk) {
                // Shutdown the bookie on disk failure.
                stateTransistor.triggerShutdown(ExitCode.BOOKIE_EXCEPTION);
            }

            @Override
            public void allDisksFull() {
                // Transition to readOnly mode on all disks full
                stateTransistor.transitToReadOnly();
            }

            @Override
            public void fatalError() {
                LOG.error("Fatal error reported by ledgerDirsManager");
                stateTransistor.triggerShutdown(ExitCode.BOOKIE_EXCEPTION);
            }
        };
    }

    //
    // Sync Thread
    //

    final static Future<Boolean> SUCCESS_FUTURE = new Future<Boolean>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public Boolean get() {
            return true;
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }
    };

    static class CountDownLatchFuture<T> implements Future<T> {

        T value = null;
        volatile boolean done = false;
        CountDownLatch latch = new CountDownLatch(1);

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public T get() throws InterruptedException {
            latch.await();
            return value;
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
            if (!latch.await(timeout, unit)) {
                throw new TimeoutException("Timed out waiting for latch");
            }
            return value;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        void setDone(T value) {
            this.value = value;
            done = true;
            latch.countDown();
        }
    }

    static class FutureWriteCallback implements WriteCallback {

        CountDownLatchFuture<Boolean> result = new CountDownLatchFuture<Boolean>();

        @Override
        public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Finished writing entry {} @ ledger {} for {} : {}", new Object[] { entryId,
                        ledgerId, addr, rc });
            }
            result.setDone(0 == rc);
        }

        public Future<Boolean> getResult() {
            return result;
        }
    }

    /**
     * SyncThread is a background thread which flushes ledger index pages periodically.
     * Also it takes responsibility of garbage collecting journal files.
     *
     * <p>
     * Before flushing, SyncThread first records a log marker {journalId, journalPos} in memory,
     * which indicates entries before this log marker would be persisted to ledger files.
     * Then sync thread begins flushing ledger index pages to ledger index files, flush entry
     * logger to ensure all entries persisted to entry loggers for future reads.
     * </p>
     * <p>
     * After all data has been persisted to ledger index files and entry loggers, it is safe
     * to persist the log marker to disk. If bookie failed after persist log mark,
     * bookie is able to relay journal entries started from last log mark without losing
     * any entries.
     * </p>
     * <p>
     * Those journal files whose id are less than the log id in last log mark, could be
     * removed safely after persisting last log mark. We provide a setting to let user keeping
     * number of old journal files which may be used for manual recovery in critical disaster.
     * </p>
     */
    class SyncThread extends Thread {
        volatile boolean running = true;
        // flag to ensure sync thread will not be interrupted during flush
        final AtomicBoolean flushing = new AtomicBoolean(false);
        // make flush interval as a parameter
        final int flushInterval;

        public SyncThread(ServerConfiguration conf) {
            super("SyncThread");
            flushInterval = conf.getFlushInterval();
            LOG.debug("Flush Interval : {}", flushInterval);
        }

        private Object suspensionLock = new Object();
        private boolean suspended = false;

        /**
         * Suspend sync thread. (for testing)
         */
        @VisibleForTesting
        public void suspendSync() {
            synchronized (suspensionLock) {
                suspended = true;
            }
        }

        /**
         * Resume sync thread. (for testing)
         */
        @VisibleForTesting
        public void resumeSync() {
            synchronized (suspensionLock) {
                suspended = false;
                suspensionLock.notify();
            }
        }

        @Override
        public void run() {
            try {
                while (running) {
                    synchronized (this) {
                        try {
                            wait(flushInterval);
                            if (!ledgerStorage.isFlushRequired()) {
                                continue;
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            continue;
                        }
                    }
                    synchronized (suspensionLock) {
                        while (suspended) {
                            suspensionLock.wait();
                        }
                    }
                    // try to mark flushing flag to make sure it would not be
                    // interrupted
                    // by shutdown during flushing. otherwise it will receive
                    // ClosedByInterruptException which may cause index file &
                    // entry logger
                    // closed and corrupted.
                    if (!flushing.compareAndSet(false, true)) {
                        // set flushing flag failed, means flushing is true now
                        // indicates another thread wants to interrupt sync
                        // thread to exit
                        break;
                    }

                    // journal mark log
                    journal.markLog();

                    boolean flushFailed = false;
                    try {
                        ledgerStorage.flush();
                    } catch (NoWritableLedgerDirException e) {
                        flushFailed = true;
                        flushing.set(false);
                        stateTransistor.transitToReadOnly();
                    } catch (IOException e) {
                        LOG.error("Exception flushing Ledger", e);
                        flushFailed = true;
                    }

                    // if flush failed, we should not roll last mark, otherwise
                    // we would
                    // have some ledgers are not flushed and their journal
                    // entries were lost
                    if (!flushFailed) {
                        try {
                            journal.rollLog();
                            journal.gcJournals();
                        } catch (NoWritableLedgerDirException e) {
                            flushing.set(false);
                            stateTransistor.transitToReadOnly();
                        }
                    }

                    // clear flushing flag
                    flushing.set(false);
                }
            } catch (Throwable t) {
                LOG.error("Exception in SyncThread", t);
                flushing.set(false);
                stateTransistor.triggerShutdown(ExitCode.BOOKIE_EXCEPTION);
            }
        }

        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            if (flushing.compareAndSet(false, true)) {
                // if setting flushing flag succeed, means syncThread is not flushing now
                // it is safe to interrupt itself now
                this.interrupt();
            }
            this.join();
        }
    }

    public InterleavedBookieStore(ServerConfiguration conf, ZooKeeper zk, StateTransistor transistor)
            throws IOException, KeeperException, InterruptedException, BookieException {
        this.conf = conf;
        this.stateTransistor = transistor;

        // Check the cookies and directories.
        this.journalDirectory = getCurrentDirectory(conf.getJournalDir());
        this.ledgerDirsManager = new LedgerDirsManager(conf);
        checkEnvironment(zk);

        // initialize the metadata manager
        ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(conf, zk);
        LOG.info("instantiate ledger manager {}", ledgerManagerFactory.getClass().getName());
        ledgerManager = ledgerManagerFactory.newLedgerManager();

        // initialize the data storage
        syncThread = new SyncThread(conf);
        ledgerStorage = new InterleavedLedgerStorage(conf, ledgerManager, ledgerDirsManager,
                new BookieSafeEntryAdder());
        handles = new HandleFactoryImpl(ledgerStorage);
        // instantiate the journal
        journal = new Journal(conf, ledgerDirsManager);
    }

    void readJournal() throws IOException, BookieException {
        journal.replay(new JournalScanner() {
            @Override
            public void process(int journalVersion, long offset, ByteBuffer recBuff) throws IOException {
                long ledgerId = recBuff.getLong();
                long entryId = recBuff.getLong();
                try {
                    LOG.debug("Replay journal - ledger id : {}, entry id : {}.", ledgerId, entryId);
                    if (entryId == METAENTRY_ID_LEDGER_KEY) {
                        if (journalVersion >= 3) {
                            int masterKeyLen = recBuff.getInt();
                            byte[] masterKey = new byte[masterKeyLen];

                            recBuff.get(masterKey);
                            masterKeyCache.put(ledgerId, masterKey);
                        } else {
                            throw new IOException("Invalid journal. Contains journalKey "
                                    + " but layout version (" + journalVersion
                                    + ") is too old to hold this");
                        }
                    } else if (entryId == METAENTRY_ID_FENCE_KEY) {
                        if (journalVersion >= 4) {
                            byte[] key = masterKeyCache.get(ledgerId);
                            if (key == null) {
                                key = ledgerStorage.readMasterKey(ledgerId);
                            }
                            LedgerDescriptor handle = handles.getHandle(ledgerId, key);
                            handle.setFenced();
                        } else {
                            throw new IOException("Invalid journal. Contains fenceKey "
                                    + " but layout version (" + journalVersion + ") is too old to hold this");
                        }
                    } else {
                        byte[] key = masterKeyCache.get(ledgerId);
                        if (key == null) {
                            key = ledgerStorage.readMasterKey(ledgerId);
                        }
                        LedgerDescriptor handle = handles.getHandle(ledgerId, key);

                        recBuff.rewind();
                        handle.addEntry(recBuff);
                    }
                } catch (NoLedgerException nsle) {
                    LOG.debug("Skip replaying entries of ledger {} since it was deleted.", ledgerId);
                } catch (BookieException be) {
                    throw new IOException(be);
                }
            }
        });
    }

    @Override
    synchronized public void start() {
        LOG.debug("I'm starting a bookie with journal directory {}", journalDirectory.getName());
        // replay journals
        try {
            readJournal();
        } catch (IOException ioe) {
            LOG.error("Exception while replaying journals, shutting down", ioe);
            stateTransistor.triggerShutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        } catch (BookieException be) {
            LOG.error("Exception while replaying journals, shutting down", be);
            stateTransistor.triggerShutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        }

        // Start DiskChecker thread
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        ledgerDirsManager.start();

        ledgerStorage.start();
        syncThread.start();
    }

    @Override
    public void run() {
        // bookie thread wait for journal thread
        try {
            // start journal
            journal.start();
            // wait until journal quits
            journal.join();
        } catch (InterruptedException ie) {
        }
        // if the journal thread quits due to shutting down, it is ok
        if (!shuttingdown) {
            // some error found in journal thread and it quits
            // following add operations to it would hang unit client timeout
            // so we should let bookie server exists
            LOG.error("Journal manager quits unexpectedly.");
            stateTransistor.triggerShutdown(ExitCode.BOOKIE_EXCEPTION);
        }
    }

    // internal shutdown method to let shutdown bookie gracefully
    // when encountering exception
    @Override
    public synchronized void shutdown() {
        try {
            if (shuttingdown) {
                return;
            }
            // mark bookie as in shutting down progress
            shuttingdown = true;

            //Shutdown disk checker
            ledgerDirsManager.shutdown();

            // Shutdown journal
            journal.shutdown();
            journal.join();
            syncThread.shutdown();

            // Shutdown the EntryLogger which has the GarbageCollector Thread running
            ledgerStorage.shutdown();

            // close Ledger Manager
            try {
                ledgerManager.close();
                ledgerManagerFactory.uninitialize();
            } catch (IOException ie) {
                LOG.error("Failed to close active ledger manager : ", ie);
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted during shutting down bookie : ", ie);
        }
    }

    public static boolean format(ServerConfiguration conf) {
        File journalDir = conf.getJournalDir();
        if (journalDir.exists() && journalDir.isDirectory()
                && journalDir.list().length != 0) {

        }
        if (!cleanDir(journalDir)) {
            LOG.error("Formatting journal directory failed");
            return false;
        }

        File[] ledgerDirs = conf.getLedgerDirs();
        for (File dir : ledgerDirs) {
            if (!cleanDir(dir)) {
                LOG.error("Formatting ledger directory " + dir + " failed");
                return false;
            }
        }
        LOG.info("Bookie format completed successfully");
        return true;
    }

    private static boolean cleanDir(File dir) {
        if (dir.exists()) {
            for (File child : dir.listFiles()) {
                boolean delete = FileUtils.deleteQuietly(child);
                if (!delete) {
                    LOG.error("Not able to delete " + child);
                    return false;
                }
            }
        } else if (!dir.mkdirs()) {
            LOG.error("Not able to create the directory " + dir);
            return false;
        }
        return true;
    }

    //
    // Requests Processing
    //

    /**
     * Retrieve the ledger descriptor for the ledger which entry should be added
     * to. The LedgerDescriptor returned from this method should be eventually
     * freed with #putHandle().
     * 
     * @throws BookieException
     *             if masterKey does not match the master key of the ledger
     */
    private LedgerDescriptor getLedgerForEntry(ByteBuffer entry, byte[] masterKey)
            throws IOException, BookieException {
        long ledgerId = entry.getLong();
        LedgerDescriptor l = handles.getHandle(ledgerId, masterKey);
        if (!masterKeyCache.containsKey(ledgerId)) {
            // new handle, we should add the key to journal ensure we can rebuild
            ByteBuffer bb = ByteBuffer.allocate(8 + 8 + 4 + masterKey.length);
            bb.putLong(ledgerId);
            bb.putLong(METAENTRY_ID_LEDGER_KEY);
            bb.putInt(masterKey.length);
            bb.put(masterKey);
            bb.flip();

            journal.logAddEntry(bb, new NopWriteCallback(), null);
            masterKeyCache.put(ledgerId, masterKey);
        }
        return l;
    }

    protected void addEntryByLedgerId(long ledgerId, ByteBuffer entry)
            throws IOException, BookieException {
        byte[] key = ledgerStorage.readMasterKey(ledgerId);
        LedgerDescriptor handle = handles.getHandle(ledgerId, key);
        handle.addEntry(entry);
    }

    /**
     * Add an entry to a ledger as specified by handle.
     */
    private void addEntryInternal(LedgerDescriptor handle, ByteBuffer entry, WriteCallback cb, Object ctx)
            throws IOException, BookieException {
        long ledgerId = handle.getLedgerId();
        entry.rewind();
        long entryId = handle.addEntry(entry);

        entry.rewind();
        LOG.trace("Adding {}@{}", entryId, ledgerId);
        journal.logAddEntry(entry, cb, ctx);
    }

    @Override
    public void addEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        try {
            LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
            synchronized (handle) {
                if (handle.isFenced()) {
                    throw BookieException
                    .create(BookieException.Code.LedgerFencedException);
                }
                addEntryInternal(handle, entry, cb, ctx);
            }
        } catch (NoWritableLedgerDirException e) {
            stateTransistor.transitToReadOnly();
            throw new IOException(e);
        }
    }

    @Override
    public void recoveryAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        try {
            LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
            synchronized (handle) {
                addEntryInternal(handle, entry, cb, ctx);
            }
        } catch (NoWritableLedgerDirException e) {
            stateTransistor.transitToReadOnly();
            throw new IOException(e);
        }
    }

    @Override
    public Future<Boolean> fenceLedger(long ledgerId, byte[] masterKey) throws IOException, BookieException {
        LedgerDescriptor handle = handles.getHandle(ledgerId, masterKey);
        boolean success;
        synchronized (handle) {
            success = handle.setFenced();
        }
        if (success) {
            // fenced first time, we should add the key to journal ensure we can
            // rebuild
            ByteBuffer bb = ByteBuffer.allocate(8 + 8);
            bb.putLong(ledgerId);
            bb.putLong(METAENTRY_ID_FENCE_KEY);
            bb.flip();

            FutureWriteCallback fwc = new FutureWriteCallback();
            LOG.debug("record fenced state for ledger {} in journal.", ledgerId);
            journal.logAddEntry(bb, fwc, null);
            return fwc.getResult();
        } else {
            // already fenced
            return SUCCESS_FUTURE;
        }
    }

    @Override
    public ByteBuffer readEntry(long ledgerId, long entryId) throws IOException, NoLedgerException {
        LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
        LOG.trace("Reading {}@{}", entryId, ledgerId);
        return handle.readEntry(entryId);
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        ledgerStorage.deleteLedger(ledgerId);
    }

    //
    // JMX things
    //

    class InterleavedBookieStoreBean implements BKMBeanInfo {

        @Override
        public String getName() {
            return "InterleavedBookieStore";
        }

        @Override
        public boolean isHidden() {
            return false;
        }

        public int getQueueLength() {
            return journal.getJournalQueueLength();
        }

    }

    @Override
    public void registerJMX(BKMBeanInfo parent) {
        try {
            jmxLedgerStorageBean = this.ledgerStorage.getJMXBean();
            BKMBeanRegistry.getInstance().register(jmxLedgerStorageBean, parent);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX for ledger cache", e);
            jmxLedgerStorageBean = null;
        }
    }

    @Override
    public void unregisterJMX() {
        try {
            if (jmxLedgerStorageBean != null) {
                BKMBeanRegistry.getInstance().unregister(jmxLedgerStorageBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX : ", e);
        }
        jmxLedgerStorageBean = null;
    }

    private class BookieSafeEntryAdder implements SafeEntryAdder {
        @Override
        public void safeAddEntry(final long ledgerId, final ByteBuffer buffer, final GenericCallback<Void> cb) {
            journal.logAddEntry(buffer, new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId2, long entryId, InetSocketAddress addr,
                        Object ctx) {
                    if (rc != BookieException.Code.OK) {
                        LOG.error("Error rewriting to journal (ledger {}, entry {})", ledgerId2, entryId);
                        cb.operationComplete(rc, null);
                        return;
                    }
                    try {
                        addEntryByLedgerId(ledgerId, buffer);
                        cb.operationComplete(rc, null);
                    } catch (IOException ioe) {
                        LOG.error("Error adding to ledger storage (ledger " + ledgerId2 + ", entry "
                                + entryId + ")", ioe);
                        // couldn't add to ledger storage
                        cb.operationComplete(BookieException.Code.IllegalOpException, null);
                    } catch (BookieException bke) {
                        LOG.error("Bookie error adding to ledger storage (ledger " + ledgerId2 + ", entry "
                                + entryId + ")", bke);
                        // couldn't add to ledger storage
                        cb.operationComplete(bke.getCode(), null);
                    }
                }
            }, null);
        }
    }

}
