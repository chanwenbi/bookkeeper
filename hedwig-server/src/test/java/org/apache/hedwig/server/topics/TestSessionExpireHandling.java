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
package org.apache.hedwig.server.topics;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.LoggingExceptionHandler;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.server.meta.ZkMetadataManagerFactory;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.hedwig.server.persistence.BookKeeperTestBase;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test session expire handling.
 */
@RunWith(Parameterized.class)
public class TestSessionExpireHandling extends HedwigHubTestBase {

    static Logger logger = LoggerFactory.getLogger(TestSessionExpireHandling.class);

    static class TestZooKeeperClient extends ZooKeeperClient {

        final CountDownLatch reconnectLatch = new CountDownLatch(1);
        final CountDownLatch expireLatch = new CountDownLatch(1);
        final int sessionTimeoutMs;

        TestZooKeeperClient(String connectString, int sessionTimeoutMs, ZooKeeperWatcherBase watcherManager,
                            RetryPolicy operationRetryPolicy) throws IOException {
            super(connectString, sessionTimeoutMs, watcherManager, operationRetryPolicy);
            this.sessionTimeoutMs = sessionTimeoutMs;
        }

        void startReconnect() {
            reconnectLatch.countDown();
        }

        void waitUntilExpired() throws InterruptedException {
            expireLatch.await(sessionTimeoutMs, TimeUnit.MILLISECONDS);
            // sleep for one more seconds to let expired state propagated
            TimeUnit.SECONDS.sleep(1);
        }

        @Override
        protected void onExpired() {
            expireLatch.countDown();
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        reconnectLatch.await();
                    } catch (InterruptedException ie) {
                    }
                    TestZooKeeperClient.super.onExpired();
                }
            };
            t.start();
        }
    }

    static class TestPubSubServer extends PubSubServer {

        TestZooKeeperClient zkClient;

        public TestPubSubServer(final ServerConfiguration serverConf,
                                final ClientConfiguration clientConf,
                                final Thread.UncaughtExceptionHandler exceptionHandler)
            throws ConfigurationException {
            super(serverConf, clientConf, exceptionHandler);
        }

        public TestZooKeeperClient getZooKeeper() {
            return zkClient;
        }

        @Override
        protected void instantiateZookeeperClient() throws Exception {
            if (conf.isStandalone()) {
                return;
            }
            ZooKeeperWatcherBase watcherManager = new ZooKeeperWatcherBase(conf.getZkTimeout());
            zkClient = new TestZooKeeperClient(conf.getZkHost(), conf.getZkTimeout(), watcherManager,
                new BoundExponentialBackoffRetryPolicy(conf.getZkTimeout(), conf.getZkTimeout(),
                                                       conf.getZkMaxRetries()));
            watcherManager.waitForConnection();
            zk = zkClient;
        }

    }

    class TestHubServerConfiguration extends HubServerConfiguration {

        TestHubServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }

        public Class<? extends MetadataManagerFactory> getMetadataManagerFactoryClass() {
            return ZkMetadataManagerFactory.class;
        }

        public boolean isMetadataManagerBasedTopicManagerEnabled() {
            return isMetadataManagerBasedTopicManagerEnabled;
        }
    }

    static class ResetableHubClientConfiguration extends ClientConfiguration {

        HedwigSocketAddress addr;

        ResetableHubClientConfiguration(HedwigSocketAddress addr) {
            this.addr = addr;
        }

        ResetableHubClientConfiguration setDefaultSocketAddress(HedwigSocketAddress addr) {
            this.addr = addr;
            return this;
        }

        @Override
        public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
            return this.addr;
        }
    }

    protected boolean isMetadataManagerBasedTopicManagerEnabled;

    public TestSessionExpireHandling(boolean isMetadataManagerBasedTopicManagerEnabled) {
        super(1);
        this.isMetadataManagerBasedTopicManagerEnabled = isMetadataManagerBasedTopicManagerEnabled;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { false }, { true }
        });
    }

    @Override
    protected ServerConfiguration getServerConfiguration(int serverPort, int sslServerPort) {
        return new TestHubServerConfiguration(serverPort, sslServerPort);
    }

    @Override
    protected PubSubServer createPubSubServer(ServerConfiguration conf) throws Exception {
        return new TestPubSubServer(conf, new ClientConfiguration(), new LoggingExceptionHandler());
    }

    private void expireHubServer(TestPubSubServer hub, int timeout) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper newZk = new ZooKeeper(bktb.getZkHostPort(), timeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.None &&
                    event.getState() == KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }
        }, hub.getZooKeeper().getSessionId(), hub.getZooKeeper().getSessionPasswd());
        if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
        }
        newZk.close();
    }

    /**
     * When ession expired, if no other hub server claimed the ownership
     * it would survive to reclaim its ownership again.
     */
    @Test(timeout=60000)
    public void testSurviveAfterSessionExpired() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestSurviveAfterSessionExpired"); 
        ByteString subid = ByteString.copyFromUtf8("subid");

        HedwigSocketAddress hub1Addr = serverAddresses.get(0);
        TestPubSubServer hub1 = (TestPubSubServer) serversList.get(0);

        ResetableHubClientConfiguration conf1 = new ResetableHubClientConfiguration(hub1Addr);
        HedwigClient client1 = new HedwigClient(conf1);
        Publisher publisher1 = client1.getPublisher();
        Subscriber subscriber1 = client1.getSubscriber();

        final int numMsgs = 4;
        final ByteString[] msgs = new ByteString[numMsgs];
        final CountDownLatch[] msgLatches = new CountDownLatch[numMsgs];
        for (int i=0; i<numMsgs; i++) {
            msgs[i] = ByteString.copyFromUtf8("Message " + i + " " + MathUtils.now());
            msgLatches[i] = new CountDownLatch(1);
        }
        final AtomicInteger numReceivedMsgs = new AtomicInteger(0);
        // subscribe the topic to cause hub1 to own the topic.
        subscriber1.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
        subscriber1.startDelivery(topic, subid, new MessageHandler() {
            @Override
            public synchronized void deliver(ByteString topic, ByteString subid, Message msg,
                                             Callback<Void> callback, Object context) {
                int expectedMsgId = numReceivedMsgs.get();
                if (msgs[expectedMsgId].equals(msg.getBody())) {
                    msgLatches[expectedMsgId].countDown();
                    numReceivedMsgs.incrementAndGet();
                }
            }
        });
        publisher1.publish(topic, Message.newBuilder().setBody(msgs[0]).build());
        assertTrue("Subscriber doesn't receive msg " + msgs[0].toStringUtf8() + " after 2 seconds",
                   msgLatches[0].await(2, TimeUnit.SECONDS));

        // expire session
        expireHubServer(hub1, 10000);
        hub1.getZooKeeper().waitUntilExpired();

        // publish should failed for a new topic
        ByteString newTopic = ByteString.copyFromUtf8("newTopic");
        try {
            publisher1.publish(newTopic, Message.newBuilder().setBody(msgs[1]).build());
            fail("Should fail to publish a message to new topic when zookeeper is session expired.");
        } catch (PubSubException.ServiceDownException sde) {
        }

        // publish should succeed
        publisher1.publish(topic, Message.newBuilder().setBody(msgs[1]).build());
        assertTrue("Subscriber doesn't receive msg " + msgs[1].toStringUtf8() + " after 2 seconds",
                   msgLatches[1].await(2, TimeUnit.SECONDS));

        // let zookeeper session resume
        hub1.getZooKeeper().startReconnect();
        // wait for a session time for zookeeper client reconnect again
        TimeUnit.SECONDS.sleep(6);

        // start a new hub server
        startNewHubServer();

        HedwigSocketAddress hub2Addr = serverAddresses.get(1);

        ResetableHubClientConfiguration conf2 = new ResetableHubClientConfiguration(hub2Addr);
        HedwigClient client2 = new HedwigClient(conf2);
        Publisher publisher2 = client2.getPublisher();

        // publish should succeed through hub2, since the owner is still hub1
        publisher2.publish(topic, Message.newBuilder().setBody(msgs[2]).build());
        assertTrue("Subscriber doesn't receive msg " + msgs[2].toStringUtf8() + " after 2 seconds",
                   msgLatches[2].await(2, TimeUnit.SECONDS));

        // publish should succeed through hub1, since the owner is still hub1
        publisher2.publish(topic, Message.newBuilder().setBody(msgs[3]).build());
        assertTrue("Subscriber doesn't receive msg " + msgs[3].toStringUtf8() + " after 2 seconds",
                   msgLatches[3].await(2, TimeUnit.SECONDS));

        client1.close();
        client2.close();
    }

    /**
     * When ession expired, if other hub server claimed the ownership
     * All its subscription channel will connect to right owner again after session is established again.
     */
    @Test(timeout=60000)
    public void testTopicOwnerChangedWhenExpired() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestTopicOwnerChangedWhenExpired"); 
        ByteString subid = ByteString.copyFromUtf8("subid");

        HedwigSocketAddress hub1Addr = serverAddresses.get(0);
        TestPubSubServer hub1 = (TestPubSubServer) serversList.get(0);

        ResetableHubClientConfiguration conf1 = new ResetableHubClientConfiguration(hub1Addr);
        HedwigClient client1 = new HedwigClient(conf1);
        Publisher publisher1 = client1.getPublisher();
        Subscriber subscriber1 = client1.getSubscriber();

        final int numMsgs = 4;
        final ByteString[] msgs = new ByteString[numMsgs];
        final CountDownLatch[] msgLatches = new CountDownLatch[numMsgs];
        for (int i=0; i<numMsgs; i++) {
            msgs[i] = ByteString.copyFromUtf8("Message " + i + " " + MathUtils.now());
            msgLatches[i] = new CountDownLatch(1);
        }
        final AtomicInteger numReceivedMsgs = new AtomicInteger(0);
        // subscribe the topic to cause hub1 to own the topic.
        subscriber1.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
        subscriber1.startDelivery(topic, subid, new MessageHandler() {
            @Override
            public synchronized void deliver(ByteString topic, ByteString subid, Message msg,
                                             Callback<Void> callback, Object context) {
                int expectedMsgId = numReceivedMsgs.get();
                if (msgs[expectedMsgId].equals(msg.getBody())) {
                    msgLatches[expectedMsgId].countDown();
                    numReceivedMsgs.incrementAndGet();
                }
            }
        });
        publisher1.publish(topic, Message.newBuilder().setBody(msgs[0]).build());
        assertTrue("Subscriber doesn't receive msg " + msgs[0].toStringUtf8() + " after 2 seconds",
                   msgLatches[0].await(2, TimeUnit.SECONDS));

        // expire session
        expireHubServer(hub1, 10000);
        hub1.getZooKeeper().waitUntilExpired();

        // publish should failed for a new topic
        ByteString newTopic = ByteString.copyFromUtf8("newTopic");
        try {
            publisher1.publish(newTopic, Message.newBuilder().setBody(msgs[1]).build());
            fail("Should fail to publish a message to new topic when zookeeper is session expired.");
        } catch (PubSubException.ServiceDownException sde) {
        }

        // publish should succeed
        publisher1.publish(topic, Message.newBuilder().setBody(msgs[1]).build());
        assertTrue("Subscriber doesn't receive msg " + msgs[1].toStringUtf8() + " after 2 seconds",
                   msgLatches[1].await(2, TimeUnit.SECONDS));

        // start a new hub server
        startNewHubServer();

        HedwigSocketAddress hub2Addr = serverAddresses.get(1);

        ResetableHubClientConfiguration conf2 = new ResetableHubClientConfiguration(hub2Addr);
        HedwigClient client2 = new HedwigClient(conf2);
        Publisher publisher2 = client2.getPublisher();

        // publish should succeed through hub2, ownership changed but subscriber doesn't be notified
        // so subscribe should not receive such message
        publisher2.publish(topic, Message.newBuilder().setBody(msgs[2]).build());
        assertFalse("Subscriber should not receive msg " + msgs[2].toStringUtf8() + " after 2 seconds"
                    + " since ownership is changed.", msgLatches[2].await(2, TimeUnit.SECONDS));

        conf1.setDefaultSocketAddress(hub2Addr);
        // let zookeeper session resume
        hub1.getZooKeeper().startReconnect();
        // wait for a session time for zookeeper client reconnect again
        // and it would find that the topic is claimed by other hub server and release its owned topic
        TimeUnit.SECONDS.sleep(6);

        assertTrue("Subscriber doesn't receive msg " + msgs[2].toStringUtf8() + " after 2 seconds",
                   msgLatches[2].await(2, TimeUnit.SECONDS));

        // publish should succeed through hub2, since the owner is changed to hub2
        publisher2.publish(topic, Message.newBuilder().setBody(msgs[3]).build());
        assertTrue("Subscriber doesn't receive msg " + msgs[3].toStringUtf8() + " after 2 seconds",
                   msgLatches[3].await(2, TimeUnit.SECONDS));

        client1.close();
        client2.close();
    }

    /**
     * When ession expired, if other hub server claimed the ownership
     * If a publish to old hub server will cause topic to be released, and redirect all subscription
     * channels to right owner.
     */
    @Test(timeout=60000)
    public void testPublishWhenExpired() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("TestPublishWhenExpired"); 
        ByteString subid = ByteString.copyFromUtf8("subid");

        HedwigSocketAddress hub1Addr = serverAddresses.get(0);
        TestPubSubServer hub1 = (TestPubSubServer) serversList.get(0);

        ResetableHubClientConfiguration conf1 = new ResetableHubClientConfiguration(hub1Addr);
        HedwigClient client1 = new HedwigClient(conf1);
        Publisher publisher1 = client1.getPublisher();
        Subscriber subscriber1 = client1.getSubscriber();

        final int numMsgs = 4;
        final ByteString[] msgs = new ByteString[numMsgs];
        final CountDownLatch[] msgLatches = new CountDownLatch[numMsgs];
        for (int i=0; i<numMsgs; i++) {
            msgs[i] = ByteString.copyFromUtf8("Message " + i + " " + MathUtils.now());
            msgLatches[i] = new CountDownLatch(1);
        }
        final AtomicInteger numReceivedMsgs = new AtomicInteger(0);
        // subscribe the topic to cause hub1 to own the topic.
        subscriber1.subscribe(topic, subid, CreateOrAttach.CREATE_OR_ATTACH);
        subscriber1.startDelivery(topic, subid, new MessageHandler() {
            @Override
            public synchronized void deliver(ByteString topic, ByteString subid, Message msg,
                                             Callback<Void> callback, Object context) {
                int expectedMsgId = numReceivedMsgs.get();
                if (msgs[expectedMsgId].equals(msg.getBody())) {
                    msgLatches[expectedMsgId].countDown();
                    numReceivedMsgs.incrementAndGet();
                }
            }
        });
        publisher1.publish(topic, Message.newBuilder().setBody(msgs[0]).build());
        assertTrue("Subscriber doesn't receive msg " + msgs[0].toStringUtf8() + " after 2 seconds",
                   msgLatches[0].await(2, TimeUnit.SECONDS));

        // expire session
        expireHubServer(hub1, 10000);
        hub1.getZooKeeper().waitUntilExpired();

        // publish should failed for a new topic
        ByteString newTopic = ByteString.copyFromUtf8("newTopic");
        try {
            publisher1.publish(newTopic, Message.newBuilder().setBody(msgs[1]).build());
            fail("Should fail to publish a message to new topic when zookeeper is session expired.");
        } catch (PubSubException.ServiceDownException sde) {
        }

        // publish should succeed
        publisher1.publish(topic, Message.newBuilder().setBody(msgs[1]).build());
        assertTrue("Subscriber doesn't receive msg " + msgs[1].toStringUtf8() + " after 2 seconds",
                   msgLatches[1].await(2, TimeUnit.SECONDS));

        // start a new hub server
        startNewHubServer();

        HedwigSocketAddress hub2Addr = serverAddresses.get(1);

        ResetableHubClientConfiguration conf2 = new ResetableHubClientConfiguration(hub2Addr);
        HedwigClient client2 = new HedwigClient(conf2);
        Publisher publisher2 = client2.getPublisher();

        // publish should succeed through hub2, ownership changed but subscriber doesn't be notified
        // so subscribe should not receive such message
        publisher2.publish(topic, Message.newBuilder().setBody(msgs[2]).build());
        assertFalse("Subscriber should not receive msg " + msgs[2].toStringUtf8() + " after 2 seconds"
                    + " since ownership is changed.", msgLatches[2].await(2, TimeUnit.SECONDS));

        conf1.setDefaultSocketAddress(hub2Addr);
        try {
            publisher1.publish(topic, Message.newBuilder().setBody(msgs[3]).build());
            fail("Should fail to publish a message when ownership changed which fenced the written ledger.");
        } catch (PubSubException.ServiceDownException sde) {
        }

        // wait for old hub server to release topic ownership and ask the subscription channel
        // to connect to the right hub server.
        TimeUnit.SECONDS.sleep(6);

        assertTrue("Subscriber doesn't receive msg " + msgs[2].toStringUtf8() + " after 2 seconds",
                   msgLatches[2].await(2, TimeUnit.SECONDS));

        // publish should succeed even through hub1, since the owner is changed to hub2
        publisher1.publish(topic, Message.newBuilder().setBody(msgs[3]).build());
        assertTrue("Subscriber doesn't receive msg " + msgs[3].toStringUtf8() + " after 2 seconds",
                   msgLatches[3].await(2, TimeUnit.SECONDS));

        client1.close();
        client2.close();
    }

}
