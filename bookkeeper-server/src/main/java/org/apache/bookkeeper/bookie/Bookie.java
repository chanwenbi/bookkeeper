/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.jmx.BKMBeanRegistry;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implements a bookie.
 *
 */
public class Bookie implements StateTransistor {

    static Logger LOG = LoggerFactory.getLogger(Bookie.class);

    private final ServerConfiguration conf;

    // ZK registration path for this bookie
    private final String bookieRegistrationPath;
    private final String bookieReadonlyRegistrationPath;

    // ZooKeeper client instance for the Bookie
    ZooKeeper zk;
    // ZK registration path for this bookie
    final private String zkBookieRegPath;
    final private String zkReadonlyRegPath;

    // Running flag
    private volatile boolean running = false;
    private int exitCode = ExitCode.OK;
    // bookie state
    final private AtomicBoolean readOnly = new AtomicBoolean(false);

    // Bookie Sotre
    BookieStore bookieStore;

    // jmx related beans
    BookieBean jmxBookieBean;

    // executor to manage the state changes for a bookie.
    final ExecutorService stateService = Executors.newSingleThreadExecutor();

    public static class NoLedgerException extends IOException {
        private static final long serialVersionUID = 1L;
        private long ledgerId;
        public NoLedgerException(long ledgerId) {
            super("Ledger " + ledgerId + " not found");
            this.ledgerId = ledgerId;
        }
        public long getLedgerId() {
            return ledgerId;
        }
    }

    public static class NoEntryException extends IOException {
        private static final long serialVersionUID = 1L;
        private long ledgerId;
        private long entryId;
        public NoEntryException(long ledgerId, long entryId) {
            this("Entry " + entryId + " not found in " + ledgerId, ledgerId, entryId);
        }

        public NoEntryException(String msg, long ledgerId, long entryId) {
            super(msg);
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        public long getLedger() {
            return ledgerId;
        }
        public long getEntry() {
            return entryId;
        }
    }

    /**
     * Return the configured address of the bookie.
     */
    public static InetSocketAddress getBookieAddress(ServerConfiguration conf)
            throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getLocalHost()
                .getHostAddress(), conf.getBookiePort());
    }

    public Bookie(ServerConfiguration conf)
            throws IOException, KeeperException, InterruptedException, BookieException {
        this.conf = conf;
        this.bookieRegistrationPath = conf.getZkAvailableBookiesPath() + "/";
        this.bookieReadonlyRegistrationPath =
                this.bookieRegistrationPath + BookKeeperConstants.READONLY;
        // ZK ephemeral node for this Bookie.
        zkBookieRegPath = this.bookieRegistrationPath + getMyId();
        zkReadonlyRegPath =
                this.bookieReadonlyRegistrationPath + "/" + getMyId();

        // instantiate zookeeper client to initialize ledger manager
        this.zk = instantiateZookeeperClient(conf);

        bookieStore = createBookieStore();
    }

    protected BookieStore createBookieStore() throws IOException, KeeperException, InterruptedException,
    BookieException {
        return new InterleavedBookieStore(conf, zk, this);
    }

    @VisibleForTesting
    public BookieStore getBookieStore() {
        return bookieStore;
    }

    private String getMyId() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();
    }

    synchronized public void start() {
        // Start the bookie Store.
        bookieStore.start();
        // set running here.
        // since bookie server use running as a flag to tell bookie server whether it is alive
        // if setting it in bookie thread, the watcher might run before bookie thread.
        running = true;
        try {
            registerBookie().get();
        } catch (Exception ie) {
            LOG.error("Couldn't register bookie with zookeeper, shutting down : ", ie);
            shutdown(ExitCode.ZK_REG_FAIL);
        }
    }

    public void join() throws InterruptedException {
        ((InterleavedBookieStore) bookieStore).join();
    }

    /**
     * Register jmx with parent
     *
     * @param parent parent bk mbean info
     */
    public void registerJMX(BKMBeanInfo parent) {
        try {
            jmxBookieBean = new BookieBean(this);
            BKMBeanRegistry.getInstance().register(jmxBookieBean, parent);

            bookieStore.registerJMX(jmxBookieBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxBookieBean = null;
        }
    }

    /**
     * Unregister jmx
     */
    public void unregisterJMX() {
        bookieStore.unregisterJMX();
        try {
            if (jmxBookieBean != null) {
                BKMBeanRegistry.getInstance().unregister(jmxBookieBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxBookieBean = null;
    }


    /**
     * Instantiate the ZooKeeper client for the Bookie.
     */
    private ZooKeeper instantiateZookeeperClient(ServerConfiguration conf)
            throws IOException, InterruptedException, KeeperException {
        if (conf.getZkServers() == null) {
            LOG.warn("No ZK servers passed to Bookie constructor so BookKeeper clients won't know about this server!");
            return null;
        }
        // Create the ZooKeeper client instance
        return newZookeeper(conf.getZkServers(), conf.getZkTimeout());
    }

    /**
     * Register as an available bookie
     */
    protected Future<Void> registerBookie() {
        return stateService.submit(new Callable<Void>() {
            @Override
            public Void call() {
                try {
                    doRegisterBookie();
                } catch (IOException ie) {
                    LOG.error("Couldn't register bookie with zookeeper, shutting down", ie);
                    shutdown(ExitCode.ZK_REG_FAIL);
                }
                return null;
            }
        });
    }

    protected void doRegisterBookie() throws IOException {
        if (null == zk) {
            // zookeeper instance is null, means not register itself to zk
            return;
        }

        String regPath = readOnly.get() ? zkReadonlyRegPath : zkBookieRegPath;

        // ZK ephemeral node for this Bookie.
        final CountDownLatch prevNodeLatch = new CountDownLatch(1);
        try{
            Watcher zkPrevRegNodewatcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // Check for prev znode deletion. Connection expiration is
                    // not handling, since bookie has logic to shutdown.
                    if (EventType.NodeDeleted == event.getType()) {
                        prevNodeLatch.countDown();
                    }
                }
            };
            Stat prevNodeState = zk.exists(regPath, zkPrevRegNodewatcher);
            // when exists call returned, we should already have a new session
            // it is safe to check whether the owner session is current session or not
            // if it was not current session, wait for session timeout.
            if (null != prevNodeState) {
                if (readOnly.get() && prevNodeState.getEphemeralOwner() == zk.getSessionId()) {
                    // the znode is created by transition to readOnly, do nothing just quit
                    return;
                }
                LOG.info("Previous bookie registration znode: "
                        + zkBookieRegPath
                        + " exists, so waiting zk sessiontimeout: "
                        + conf.getZkTimeout() + "ms for znode deletion");
                // waiting for the previous bookie reg znode deletion
                if (!prevNodeLatch.await(conf.getZkTimeout(),
                        TimeUnit.MILLISECONDS)) {
                    throw new KeeperException.NodeExistsException(
                            zkBookieRegPath);
                }
            }

            // Create the ZK ephemeral node for this Bookie.
            zk.create(regPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        } catch (KeeperException ke) {
            LOG.error("ZK exception registering ephemeral Znode for Bookie!",
                    ke);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new IOException(ke);
        } catch (InterruptedException ie) {
            LOG.error("ZK exception registering ephemeral Znode for Bookie!",
                    ie);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new IOException(ie);
        }
    }

    /*
     * Transition the bookie to readOnly mode
     */
    @Override
    public void transitToReadOnly() {
        try {
            transitionToReadyOnlyMode().get();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted transistion to read only mode : ", e);
        } catch (ExecutionException e) {
            LOG.warn("Failed to transit to read only mode : ", e);
        }
    }

    private Future<Void> transitionToReadyOnlyMode() {
        return stateService.submit(new Callable<Void>() {
            @Override
            public Void call() {
                doTransitionToReadOnlyMode();
                return null;
            }
        });
    }

    void doTransitionToReadOnlyMode() {
        if (!readOnly.compareAndSet(false, true)) {
            return;
        }
        if (!conf.isReadOnlyModeEnabled()) {
            LOG.warn("ReadOnly mode is not enabled. "
                    + "Can be enabled by configuring "
                    + "'readOnlyModeEnabled=true' in configuration."
                    + "Shutting down bookie");
            triggerShutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        }
        LOG.info("Transitioning Bookie to ReadOnly mode,"
                + " and will serve only read requests from clients!");
        // change zookeeper state only when using zookeeper
        if (null == zk) {
            return;
        }
        try {
            if (null == zk.exists(this.bookieReadonlyRegistrationPath, false)) {
                try {
                    zk.create(this.bookieReadonlyRegistrationPath, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (NodeExistsException e) {
                    // this node is just now created by someone.
                }
            }
            // Create the readonly node
            zk.create(this.zkReadonlyRegPath,
                    new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            try {
                // Clear the current registered node
                zk.delete(zkBookieRegPath, -1);
            } catch (NoNodeException nne) {
                // this node is remove due to session expired.
            }
        } catch (KeeperException e) {
            LOG.error("Error in transition to ReadOnly Mode."
                    + " Shutting down", e);
            triggerShutdown(ExitCode.BOOKIE_EXCEPTION);
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted Exception while transitioning to ReadOnly Mode.");
            return;
        }
    }

    /*
     * Check whether Bookie is writable
     */
    public boolean isReadOnly() {
        return readOnly.get();
    }

    /**
     * Create a new zookeeper client to zk cluster.
     *
     * <p>
     * Bookie Server just used zk client when syncing ledgers for garbage collection.
     * So when zk client is expired, it means this bookie server is not available in
     * bookie server list. The bookie client will be notified for its expiration. No
     * more bookie request will be sent to this server. So it's better to exit when zk
     * expired.
     * </p>
     * <p>
     * Since there are lots of bk operations cached in queue, so we wait for all the operations
     * are processed and quit. It is done by calling <b>shutdown</b>.
     * </p>
     *
     * @param zkServers the quorum list of zk servers
     * @param sessionTimeout session timeout of zk connection
     *
     * @return zk client instance
     */
    private ZooKeeper newZookeeper(final String zkServers,
            final int sessionTimeout) throws IOException, InterruptedException,
            KeeperException {
        Set<Watcher> watchers = new HashSet<Watcher>();
        watchers.add(new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (!running) {
                    // do nothing until first registration
                    return;
                }
                // Check for expired connection.
                if (event.getType().equals(EventType.None) &&
                        event.getState().equals(KeeperState.Expired)) {
                    // schedule a re-register operation
                    registerBookie();
                }
            }
        });
        // TODO: user could customize backoff time?
        long baseBackoffTime = sessionTimeout;
        return ZooKeeperClient.createConnectedZooKeeperClient(
                zkServers, sessionTimeout, watchers,
                new BoundExponentialBackoffRetryPolicy(baseBackoffTime, 3 * baseBackoffTime, Integer.MAX_VALUE));
    }

    public boolean isRunning() {
        return running;
    }

    // Triggering the Bookie shutdown in its own thread,
    // because shutdown can be called from sync thread which would be
    // interrupted by shutdown call.
    @Override
    public void triggerShutdown(final int exitCode) {
        Thread shutdownThread = new Thread() {
            @Override
            public void run() {
                Bookie.this.shutdown(exitCode);
            }
        };
        shutdownThread.start();
        try {
            shutdownThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debug("InterruptedException while waiting for shutdown. Not a problem!!");
        }
    }

    // provided a public shutdown method for other caller
    // to shut down bookie gracefully
    public int shutdown() {
        return shutdown(ExitCode.OK);
    }

    // internal shutdown method to let shutdown bookie gracefully
    // when encountering exception
    synchronized int shutdown(int exitCode) {
        try {
            if (running) { // avoid shutdown twice
                // the exitCode only set when first shutdown usually due to exception found
                this.exitCode = exitCode;

                // Shutdown Bookie Store
                bookieStore.shutdown();

                // Shutdown the ZK client
                if(zk != null) zk.close();
                // Shutdown State Service
                stateService.shutdown();

                // setting running to false here, so watch thread in bookie server know it only after bookie shut down
                running = false;
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted during shutting down bookie : ", ie);
        }
        return this.exitCode;
    }

    /**
     * Add entry to a ledger, even if the ledger has previous been fenced. This should only
     * happen in bookie recovery or ledger recovery cases, where entries are being replicates
     * so that they exist on a quorum of bookies. The corresponding client side call for this
     * is not exposed to users.
     */
    public void recoveryAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        bookieStore.recoveryAddEntry(entry, cb, ctx, masterKey);
    }

    /**
     * Add entry to a ledger.
     * @throws BookieException.LedgerFencedException if the ledger is fenced
     */
    public void addEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        bookieStore.addEntry(entry, cb, ctx, masterKey);
    }

    /**
     * Fences a ledger. From this point on, clients will be unable to write to
     * this ledger. Only recoveryAddEntry will be able to add entries to the
     * ledger. This method is idempotent. Once a ledger is fenced, it can never
     * be unfenced. Fencing a fenced ledger has no effect.
     */
    public Future<Boolean> fenceLedger(long ledgerId, byte[] masterKey) throws IOException, BookieException {
        return bookieStore.fenceLedger(ledgerId, masterKey);
    }

    public ByteBuffer readEntry(long ledgerId, long entryId) throws IOException, NoLedgerException {
        return bookieStore.readEntry(ledgerId, entryId);
    }

    // The rest of the code is test stuff
    static class CounterCallback implements WriteCallback {
        int count;

        @Override
        synchronized public void writeComplete(int rc, long l, long e, InetSocketAddress addr, Object ctx) {
            count--;
            if (count == 0) {
                notifyAll();
            }
        }

        synchronized public void incCount() {
            count++;
        }

        synchronized public void waitZero() throws InterruptedException {
            while (count > 0) {
                wait();
            }
        }
    }

    /**
     * Format the bookie server data
     * 
     * @param conf
     *            ServerConfiguration
     * @param isInteractive
     *            Whether format should ask prompt for confirmation if old data
     *            exists or not.
     * @param force
     *            If non interactive and force is true, then old data will be
     *            removed without confirm prompt.
     * @return Returns true if the format is success else returns false
     */
    public static boolean format(ServerConfiguration conf,
            boolean isInteractive, boolean force) {
        try {
            boolean confirm = false;
            if (!isInteractive) {
                // If non interactive and force is set, then delete old
                // data.
                if (force) {
                    confirm = true;
                } else {
                    confirm = false;
                }
            } else {
                confirm = IOUtils.confirmPrompt("Are you sure to format Bookie data..?");
            }

            if (!confirm) {
                LOG.error("Bookie format aborted!!");
                return false;
            }
        } catch (IOException e) {
            LOG.error("Error during bookie format", e);
            return false;
        }

        // TODO: check the bookie store type.
        return InterleavedBookieStore.format(conf);
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args)
            throws IOException, InterruptedException, BookieException, KeeperException {
        Bookie b = new Bookie(new ServerConfiguration());
        b.start();
        CounterCallback cb = new CounterCallback();
        long start = MathUtils.now();
        for (int i = 0; i < 100000; i++) {
            ByteBuffer buff = ByteBuffer.allocate(1024);
            buff.putLong(1);
            buff.putLong(i);
            buff.limit(1024);
            buff.position(0);
            cb.incCount();
            b.addEntry(buff, cb, null, new byte[0]);
        }
        cb.waitZero();
        long end = MathUtils.now();
        System.out.println("Took " + (end-start) + "ms");
    }
}
