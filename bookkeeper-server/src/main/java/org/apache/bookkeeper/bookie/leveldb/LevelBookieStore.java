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
package org.apache.bookkeeper.bookie.leveldb;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieStore;
import org.apache.bookkeeper.bookie.GarbageCollector;
import org.apache.bookkeeper.bookie.GarbageCollector.GarbageCleaner;
import org.apache.bookkeeper.bookie.ScanAndCompareGarbageCollector;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class LevelBookieStore implements BookieStore {

    static final Logger LOG = LoggerFactory.getLogger(LevelBookieStore.class);

    final static byte[] EMPTY_BYTES = new byte[0];

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

    // information about a ledger
    // TODO: change to use protobuf in future.
    static class LedgerInfo {
        volatile boolean fenced;
        byte[] masterKey = EMPTY_BYTES;

        LedgerInfo() {}

        LedgerInfo(byte[] masterKey) {
            fenced = false;
            this.masterKey = masterKey;
        }

        public byte[] serialize() {
            int size = 1 + 4 + masterKey.length;
            byte[] data = new byte[size];
            ByteBuffer buf = ByteBuffer.wrap(data);
            buf.put((byte)(fenced ? 1 : 0))
            .putInt(masterKey.length)
            .put(masterKey);
            return data;
        }

        public LedgerInfo deserialize(byte[] data) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            fenced = ((byte) 1) == buf.get();
            int length = buf.getInt();
            if (length > 0) {
                masterKey = new byte[length];
            }
            buf.get(masterKey);
            return this;
        }
    }

    static final int ENTRY_KEY_SIZE = 2 * 8;
    static final int LEDGER_KEY_SIZE = 8;

    final static int BLOCK_SIZE = 8192;
    final static int MB = 1024 * 1024;
    final static long META_CACHE_SIZE = 256 * MB;
    final static long DATA_CACHE_SIZE = 2048 * MB;
    final static int META_WRITE_BUFFER_SIZE = 16 * MB;
    final static int DATA_WRITE_BUFFER_SIZE = 1024 * MB;

    final ServerConfiguration conf;

    final DB metaDB;
    final DB dataDB;
    final ReadOptions ro;
    final WriteOptions wo;

    volatile boolean running = false;

    final Cache<Long, LedgerInfo> metaCache;

    final LedgerManagerFactory ledgerManagerFactory;
    final long gcWaitTime;
    final SnapshotMap<Long, Boolean> activeLedgers;
    final GarbageCollector gcCollector;

    public LevelBookieStore(ServerConfiguration conf, ZooKeeper zk)
            throws IOException, KeeperException, InterruptedException {
        this.conf = conf;

        String dbPath = conf.geLeveldbPath();

        // create sub directories for leveldb if necessary
        File metaDir = new File(dbPath, "meta");
        if (!metaDir.exists()) {
            if (!metaDir.mkdirs()) {
                throw new IOException("Failed to create meta database at " + dbPath);
            }
        }
        File dataDir = new File(dbPath, "data");
        if (dataDir.exists()) {
            if (!dataDir.mkdirs()) {
                throw new IOException("Failed to create data database at " + dbPath);
            }
        }

        // prepare the meta cache
        metaCache = CacheBuilder.newBuilder()
                .expireAfterAccess(conf.getLevelBookieCacheTTL(), TimeUnit.SECONDS)
                .initialCapacity(conf.getLevelBookieInitialCacheLedgers())
                .maximumSize(conf.getLevelBookieMaxCacheLedgers())
                .build();

        CompressionType compressionType =
                conf.isLevelBookieCompressionEnabled() ? CompressionType.SNAPPY : CompressionType.NONE;

        Options metaOptions = new Options();
        metaOptions.blockSize(conf.getLevelBookieBlockSize());
        metaOptions.cacheSize(conf.getLevelBookieMetaCacheSize());
        metaOptions.compressionType(compressionType);
        metaOptions.createIfMissing(true);
        metaOptions.writeBufferSize(conf.getLevelBookieMetaWriteBufferSize());
        metaDB = factory.open(new File(dbPath, "meta"), metaOptions);

        Options dataOptions = new Options();
        dataOptions.blockSize(conf.getLevelBookieBlockSize());
        dataOptions.cacheSize(conf.getLevelBookieDataCacheSize());
        dataOptions.compressionType(compressionType);
        dataOptions.createIfMissing(true);
        dataOptions.writeBufferSize(conf.getLevelBookieDataWriteBufferSize());
        dataOptions.maxOpenFiles(conf.getLevelBookieMaxOpenFiles());
        dataDB = factory.open(new File(dbPath, "data"), dataOptions);

        ro = new ReadOptions();
        ro.fillCache(true);
        wo = new WriteOptions();

        // initialize the metadata manager
        ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(conf, zk);
        gcWaitTime = conf.getGcWaitTime();
        activeLedgers = new SnapshotMap<Long, Boolean>();
        this.gcCollector = new ScanAndCompareGarbageCollector(ledgerManagerFactory.newLedgerManager(),
                activeLedgers);

        LOG.info("Level Bookie Store is up at {}", dbPath);
    }

    private byte[] getEntryKey(long ledgerId, long entryId) {
        byte[] key = new byte[ENTRY_KEY_SIZE];
        ByteBuffer keyBuf = ByteBuffer.wrap(key);
        keyBuf.putLong(ledgerId);
        keyBuf.putLong(Long.MAX_VALUE - entryId);
        keyBuf.flip();
        return key;
    }

    private byte[] getLedgerKey(long ledgerId) {
        byte[] key = new byte[LEDGER_KEY_SIZE];
        ByteBuffer keyBuf = ByteBuffer.wrap(key);
        keyBuf.putLong(ledgerId);
        keyBuf.flip();
        return key;
    }

    private void checkAccess(final long ledgerId, final byte[] masterKey, boolean checkFenced)
            throws BookieException, IOException {
        LedgerInfo meta;
        try {
            meta = metaCache.get(ledgerId, new Callable<LedgerInfo>() {

                @Override
                public LedgerInfo call() throws IOException {
                    // first, check the entry from local meta db.
                    byte[] ledgerKey = getLedgerKey(ledgerId);
                    byte[] metaBytes;
                    try {
                        metaBytes = metaDB.get(ledgerKey, ro);
                    } catch (DBException dbe) {
                        throw new IOException(dbe);
                    }
                    if (null != metaBytes) {
                        return new LedgerInfo().deserialize(metaBytes);
                    }

                    // if not existed, update the entry to local meta db.
                    LedgerInfo meta = new LedgerInfo(masterKey);
                    try {
                        metaDB.put(ledgerKey, meta.serialize(), wo);
                        activeLedgers.put(ledgerId, true);
                    } catch (DBException dbe) {
                        throw new IOException(dbe);
                    }
                    return meta;
                }

            });
        } catch (ExecutionException ee) {
            throw new IOException(ee);
        }
        if (checkFenced && meta.fenced) {
            throw BookieException.create(BookieException.Code.LedgerFencedException);
        }
        if (!Arrays.equals(meta.masterKey, masterKey)) {
            throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
        }
    }

    private Future<Boolean> doFenceLedger(final long ledgerId, final byte[] masterKey)
            throws BookieException, IOException {
        LedgerInfo meta;
        try {
            meta = metaCache.get(ledgerId, new Callable<LedgerInfo>() {

                @Override
                public LedgerInfo call() throws IOException {
                    // first, check the entry from local meta db.
                    byte[] ledgerKey = getLedgerKey(ledgerId);
                    byte[] metaBytes;
                    try {
                        metaBytes = metaDB.get(ledgerKey, ro);
                    } catch (DBException dbe) {
                        throw new IOException(dbe);
                    }
                    if (null != metaBytes) {
                        return new LedgerInfo().deserialize(metaBytes);
                    }

                    // if not existed, update the entry to local meta db.
                    LedgerInfo meta = new LedgerInfo(masterKey);
                    try {
                        metaDB.put(ledgerKey, meta.serialize(), wo);
                        activeLedgers.put(ledgerId, true);
                    } catch (DBException dbe) {
                        throw new IOException(dbe);
                    }
                    return meta;
                }

            });
        } catch (ExecutionException ee) {
            throw new IOException(ee);
        }

        synchronized (meta) {
            if (!Arrays.equals(meta.masterKey, masterKey)) {
                throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
            }
            if (meta.fenced) {
                return SUCCESS_FUTURE;
            }

            meta.fenced = true;
            try {
                metaDB.put(getLedgerKey(ledgerId), meta.serialize(), wo);
            } catch (DBException dbe) {
                throw new IOException(dbe);
            }
            return SUCCESS_FUTURE;
        }
    }

    void removeLedgerMeta(long ledgerId) throws IOException {
        metaDB.delete(getLedgerKey(ledgerId), wo);
        metaCache.invalidate(ledgerId);
        activeLedgers.remove(ledgerId);
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        removeEntries(ledgerId);
        removeLedgerMeta(ledgerId);
    }

    @Override
    public void addEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();
        doAddEntry(ledgerId, entryId, entry, cb, ctx, masterKey, true);
    }

    @Override
    public void recoveryAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();
        doAddEntry(ledgerId, entryId, entry, cb, ctx, masterKey, false);
    }

    @Override
    public Future<Boolean> fenceLedger(long ledgerId, byte[] masterKey) throws IOException, BookieException {
        return doFenceLedger(ledgerId, masterKey);
    }

    @Override
    public ByteBuffer readEntry(long ledgerId, long entryId)
            throws IOException, NoLedgerException {
        return doReadEntry(ledgerId, entryId);
    }

    void doAddEntry(long ledgerId, long entryId, ByteBuffer entry,
            WriteCallback cb, Object ctx, byte[] masterKey, boolean checkFenced)
                    throws IOException, BookieException {
        checkAccess(ledgerId, masterKey, checkFenced);

        byte[] entryKey = getEntryKey(ledgerId, entryId);
        byte[] entryData = new byte[entry.limit()];
        entry.get(entryData);

        try {
            dataDB.put(entryKey, entryData, wo);
        } catch (DBException dbe) {
            throw new IOException(dbe);
        }
        cb.writeComplete(0, ledgerId, entryId, null, ctx);
    }

    private boolean isSameLedger(byte[] key, long ledgerId) {
        ByteBuffer buf = ByteBuffer.wrap(key);
        return ledgerId == buf.getLong();
    }

    ByteBuffer doReadEntry(long ledgerId, long entryId)
            throws IOException, NoLedgerException {
        if (BookieProtocol.LAST_ADD_CONFIRMED == entryId) {
            return readLastEntry(ledgerId);
        } else {
            byte[] data;
            try {
                data = dataDB.get(getEntryKey(ledgerId, entryId), ro);
                if (null != data) {
                    return ByteBuffer.wrap(data);
                }
            } catch (DBException dbe) {
                throw new IOException(dbe);
            }
            throw new NoEntryException(ledgerId, entryId);
        }
    }

    ByteBuffer readLastEntry(long ledgerId)
            throws IOException, NoLedgerException {
        DBIterator iter = null;
        try {
            iter = dataDB.iterator(ro);
            iter.seek(getEntryKey(ledgerId, Long.MAX_VALUE));
            if (iter.hasNext()) {
                Entry<byte[], byte[]> entry = iter.next();
                if (isSameLedger(entry.getKey(), ledgerId)) {
                    return ByteBuffer.wrap(entry.getValue());
                } else {
                    throw new NoLedgerException(ledgerId);
                }
            } else {
                throw new NoLedgerException(ledgerId);
            }
        } catch (DBException dbe) {
            throw new IOException(dbe);
        } finally {
            if (null != iter) {
                iter.close();
            }
        }
    }

    void removeEntries(long ledgerId) throws IOException {
        DBIterator iter = null;
        try {
            iter = dataDB.iterator(ro);
            iter.seek(getEntryKey(ledgerId, 0));
            while (iter.hasNext()) {
                Entry<byte[], byte[]> entry = iter.next();
                if (!isSameLedger(entry.getKey(), ledgerId)) {
                    break;
                }
            }
        } catch (DBException dbe) {
            throw new IOException(dbe);
        }
    }

    @Override
    public synchronized void start() {
        // bootstrap the active ledger manager
        DBIterator iterator = metaDB.iterator(ro);
        iterator.seekToFirst();
        while (iterator.hasNext()) {
            Entry<byte[], byte[]> entry = iterator.next();
            long ledgerId = ByteBuffer.wrap(entry.getValue()).getLong();
            activeLedgers.put(ledgerId, true);
        }
        running = true;
    }

    //
    // GC Logic
    //

    @Override
    public void run() {
        while (running) {
            synchronized (this) {
                try {
                    wait(gcWaitTime);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    continue;
                }
            }
            gcCollector.gc(new GarbageCleaner() {

                @Override
                public void clean(long ledgerId) {
                    try {
                        removeEntries(ledgerId);
                    } catch (IOException e) {
                        LOG.error("Exception when deleting ledger " + ledgerId + " : ", e);
                    }
                }

            });
        }
    }

    @Override
    public synchronized void shutdown() {
        if (!running) {
            return;
        }

        // finally close the db
        dataDB.close();
        metaDB.close();

        running = false;
    }

    @Override
    public void registerJMX(BKMBeanInfo parent) {
        // TODO: add jmx for leveldb in future
    }

    @Override
    public void unregisterJMX() {
        // TODO: add jmx for leveldb in future
    }

}
