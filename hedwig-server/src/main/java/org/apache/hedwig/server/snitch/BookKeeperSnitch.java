/**
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
package org.apache.hedwig.server.snitch;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.server.meta.ZkMetadataManagerFactory;
import org.apache.hedwig.server.persistence.BookkeeperPersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManagerWithRangeScan;
import org.apache.hedwig.server.persistence.ReadAheadCache;
import org.apache.hedwig.server.subscriptions.MMSubscriptionManager;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.topics.MMTopicManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.ZkTopicManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A <i>Snitch</i> implementation based on BookKeeper and ZooKeeper.
 */
public class BookKeeperSnitch extends AbstractSnitch {

    static final Logger logger = LoggerFactory.getLogger(BookKeeperSnitch.class);

    protected ZooKeeper zk;
    protected BookKeeper bk;
    // Metadata Manager Factory
    protected MetadataManagerFactory mm;

    public BookKeeperSnitch(ServerConfiguration conf, ClientConfiguration clientConf,
            ClientSocketChannelFactory clientChannelFactory) throws IOException {
        super(conf, clientConf, clientChannelFactory);
    }

    protected void instantiateZooKeeperClient() throws IOException {
        final CountDownLatch signalZkReady = new CountDownLatch(1);

        zk = new ZooKeeper(conf.getZkHost(), conf.getZkTimeout(), new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (Event.KeeperState.SyncConnected.equals(event.getState())) {
                    signalZkReady.countDown();
                }
            }
        });
        // wait until connection is effective
        try {
            if (!signalZkReady.await(conf.getZkTimeout() * 2, TimeUnit.MILLISECONDS)) {
                logger.error("Could not establish connection with ZooKeeper after zk_timeout*2 = "
                        + conf.getZkTimeout() * 2 + " ms. (Default value for zk_timeout is 2000).");
                throw new IOException("Could not establish connection with ZooKeeper after zk_timeout*2 = "
                        + conf.getZkTimeout() * 2 + " ms. (Default value for zk_timeout is 2000).");
            }
        } catch (InterruptedException e) {
            throw new IOException("Interrupted waiting zookeeper client to establish connection :", e);
        }
    }

    protected void instantiateMetadataManagerFactory() throws IOException {
        try {
            mm = MetadataManagerFactory.newMetadataManagerFactory(conf, zk);
        } catch (KeeperException e) {
            throw new IOException("Failed to create metadata manager factory : ", e);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted creating metadata manager factory : ", e);
        }
    }

    @Override
    protected void initialize() throws IOException {
        instantiateZooKeeperClient();
        instantiateMetadataManagerFactory();
        super.initialize();
    }

    @Override
    protected TopicManager instantiateTopicManager(OrderedSafeExecutor scheduler) throws IOException {
        try {
            if (conf.isMetadataManagerBasedTopicManagerEnabled()) {
                return new MMTopicManager(conf, zk, mm, scheduler);
            } else {
                if (!(mm instanceof ZkMetadataManagerFactory)) {
                    throw new IOException("Uses " + mm.getClass().getName() + " to store hedwig metadata, "
                            + "but uses zookeeper ephemeral znodes to store topic ownership. "
                            + "Check your configuration as this could lead to scalability issues.");
                }
                return new ZkTopicManager(zk, conf, scheduler);
            }
        } catch (PubSubException e) {
            logger.error("Could not instantiate TopicOwnershipManager based topic manager", e);
            throw new IOException(e);
        }
    }

    @Override
    protected PersistenceManager instantiatePersistenceManager(TopicManager topicMgr, OrderedSafeExecutor scheduler)
            throws IOException {
        try {
            org.apache.bookkeeper.conf.ClientConfiguration bkConf = new org.apache.bookkeeper.conf.ClientConfiguration();
            bkConf.addConfiguration(conf.getConf());
            bk = new BookKeeper(bkConf, zk, clientChannelFactory);
        } catch (KeeperException e) {
            logger.error("Could not instantiate bookkeeper client : ", e);
            throw new IOException(e);
        } catch (InterruptedException e) {
            logger.error("Interrupted instantiating bookkeeper client : ", e);
            throw new IOException(e);
        }
        PersistenceManagerWithRangeScan underlyingPM = new BookkeeperPersistenceManager(bk, mm, topicMgr, conf,
                scheduler);
        if (conf.getReadAheadEnabled()) {
            return new ReadAheadCache(underlyingPM, conf).start();
        } else {
            return underlyingPM;
        }
    }

    @Override
    protected SubscriptionManager instantiateSubscriptionManager(TopicManager tm, PersistenceManager pm,
            DeliveryManager dm, OrderedSafeExecutor scheduler) throws IOException {
        return new MMSubscriptionManager(conf, mm, tm, pm, dm, scheduler);
    }

    @Override
    public void stop() {
        super.stop();
        stopMetadataManagerFactory();
        stopBookKeeperClient();
        stopZooKeeperClient();
    }

    protected void stopMetadataManagerFactory() {
        // shutdown metadata manager
        if (null != mm) {
            try {
                mm.shutdown();
            } catch (IOException ie) {
                logger.error("Error while shutdown metadata manager factory : ", ie);
            }
        }
    }

    protected void stopBookKeeperClient() {
        if (null != bk) {
            try {
                bk.close();
            } catch (InterruptedException e) {
                logger.error("Interrupted closing BookKeeper client : ", e);
            } catch (BKException e) {
                logger.error("Error closing BookKeeper client : ", e);
            }
        }
    }

    protected void stopZooKeeperClient() {
        if (null != zk) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                logger.error("Interrupted closing ZooKeeper client : ", e);
            }
        }
    }

}
