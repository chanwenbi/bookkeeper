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
package org.apache.hedwig.server.snitch.helix;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.bookkeeper.test.PortManager;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.HelperMethods;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.server.LoggingExceptionHandler;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.hedwig.server.persistence.BookKeeperTestBase;
import org.apache.hedwig.server.persistence.PersistRequest;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManagerWithRangeScan;
import org.apache.hedwig.server.persistence.RangeScanRequest;
import org.apache.hedwig.server.persistence.ScanCallback;
import org.apache.hedwig.server.persistence.ScanRequest;
import org.apache.hedwig.server.snitch.SnitchSeeker;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.FileUtils;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class TestHelixPersistenceManager extends TestCase {

    static final Logger logger = LoggerFactory.getLogger(TestHelixPersistenceManager.class);

    protected BookKeeperTestBase bktb;
    protected TestPubSubServer server;
    protected File pstDir, prtDir, subDir;
    protected int NUM_MESSAGES_TO_TEST = 5;
    protected int NUM_TOPICS_TO_TEST = 5;

    protected TestCallback testCallback = new TestCallback();
    protected RuntimeException failureException;

    static class TestPubSubServer extends PubSubServer {

        public TestPubSubServer(ServerConfiguration serverConfiguration, ClientConfiguration clientConfiguration,
                UncaughtExceptionHandler exceptionHandler) throws ConfigurationException {
            super(serverConfiguration, clientConfiguration, exceptionHandler);
        }

        SnitchSeeker getSnitchSeeker() {
            return snitchSeeker;
        }

    }

    protected class TestCallback implements Callback<PubSubProtocol.MessageSeqId> {

        @Override
        public void operationFailed(Object ctx, PubSubException exception) {
            throw (failureException = new RuntimeException(exception));
        }

        @Override
        @SuppressWarnings("unchecked")
        public void operationFinished(Object ctx, PubSubProtocol.MessageSeqId resultOfOperation) {
            LinkedBlockingQueue<Boolean> statusQueue = (LinkedBlockingQueue<Boolean>) ctx;
            try {
                statusQueue.put(true);
            } catch (InterruptedException e) {
                throw (failureException = new RuntimeException(e));
            }
        }
    }

    protected class Publisher implements Runnable {
        List<Message> pubMsgs;
        ByteString topic;

        public Publisher(ByteString topic, List<Message> pubMsgs) {
            this.pubMsgs = pubMsgs;
            this.topic = topic;
        }

        @Override
        public void run() {
            LinkedBlockingQueue<Boolean> statusQueue = new LinkedBlockingQueue<Boolean>();
            int i = 0;
            for (Message msg : pubMsgs) {
                try {
                    ++i;
                    logger.info("Publish Message {} for topic {}.", i, topic.toStringUtf8());
                    server.getSnitchSeeker().getSnitch(topic).getPersistenceManager()
                            .persistMessage(new PersistRequest(topic, msg, testCallback, statusQueue));
                    // wait a maximum of a minute
                    Boolean b = statusQueue.poll(60, TimeUnit.SECONDS);
                    if (b == null) {
                        throw (failureException = new RuntimeException("Scanning timed out"));
                    }
                } catch (InterruptedException e) {
                    throw (failureException = new RuntimeException(e));
                }
            }
        }

    }

    protected class RangeScanVerifierListener implements ScanCallback {
        List<Message> pubMsgs;

        public RangeScanVerifierListener(List<Message> pubMsgs) {
            this.pubMsgs = pubMsgs;
        }

        @Override
        public void messageScanned(Object ctx, Message recvMessage) {
            if (pubMsgs.isEmpty()) {
                throw (failureException = new RuntimeException("Message received when none expected"));
            }
            Message pubMsg = pubMsgs.get(0);
            if (!HelperMethods.areEqual(recvMessage, pubMsg)) {
                throw (failureException = new RuntimeException("Scanned message not equal to expected"));
            }
            pubMsgs.remove(0);
        }

        @Override
        public void scanFailed(Object ctx, Exception exception) {
            throw (failureException = new RuntimeException(exception));
        }

        @Override
        @SuppressWarnings("unchecked")
        public void scanFinished(Object ctx, ReasonForFinish reason) {
            if (reason != ReasonForFinish.NO_MORE_MESSAGES) {
                throw (failureException = new RuntimeException("Scan finished prematurely " + reason));
            }
            LinkedBlockingQueue<Boolean> statusQueue = (LinkedBlockingQueue<Boolean>) ctx;
            try {
                statusQueue.put(true);
            } catch (InterruptedException e) {
                throw (failureException = new RuntimeException(e));
            }
        }

    }

    protected class PointScanVerifierListener implements ScanCallback {
        List<Message> pubMsgs;
        ByteString topic;
        PersistenceManager persistenceManager;

        public PointScanVerifierListener(PersistenceManager persistenceManager, List<Message> pubMsgs, ByteString topic) {
            this.persistenceManager = persistenceManager;
            this.topic = topic;
            this.pubMsgs = pubMsgs;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void messageScanned(Object ctx, Message recvMessage) {

            Message pubMsg = pubMsgs.get(0);
            if (!HelperMethods.areEqual(recvMessage, pubMsg)) {
                throw (failureException = new RuntimeException("Scanned message not equal to expected"));
            }
            pubMsgs.remove(0);

            if (pubMsgs.isEmpty()) {
                LinkedBlockingQueue<Boolean> statusQueue = (LinkedBlockingQueue<Boolean>) ctx;
                try {
                    statusQueue.put(true);
                } catch (InterruptedException e) {
                    throw (failureException = new RuntimeException(e));
                }
            } else {
                long seqId = recvMessage.getMsgId().getLocalComponent();
                seqId = persistenceManager.getSeqIdAfterSkipping(topic, seqId, 1);
                ScanRequest request = new ScanRequest(topic, seqId, new PointScanVerifierListener(persistenceManager,
                        pubMsgs, topic), ctx);
                persistenceManager.scanSingleMessage(request);
            }

        }

        @Override
        public void scanFailed(Object ctx, Exception exception) {
            throw (failureException = new RuntimeException(exception));
        }

        @Override
        public void scanFinished(Object ctx, ReasonForFinish reason) {

        }

    }

    protected class ScanVerifier implements Runnable {
        List<Message> pubMsgs;
        ByteString topic;
        LinkedBlockingQueue<Boolean> statusQueue = new LinkedBlockingQueue<Boolean>();

        public ScanVerifier(ByteString topic, List<Message> pubMsgs) {
            this.topic = topic;
            this.pubMsgs = pubMsgs;
        }

        @Override
        public void run() {
            // start the scan
            PersistenceManager persistenceManager = server.getSnitchSeeker().getSnitch(topic).getPersistenceManager();
            try {
                if (persistenceManager instanceof PersistenceManagerWithRangeScan) {
                    ScanCallback listener = new RangeScanVerifierListener(pubMsgs);
                    PersistenceManagerWithRangeScan rangePersistenceManager = (PersistenceManagerWithRangeScan) persistenceManager;
                    rangePersistenceManager.scanMessages(new RangeScanRequest(topic, 1,
                            NUM_MESSAGES_TO_TEST + 1, Long.MAX_VALUE, listener, statusQueue));
                } else {
                    ScanCallback listener = new PointScanVerifierListener(persistenceManager, pubMsgs, topic);
                    persistenceManager.scanSingleMessage(new ScanRequest(topic, 1, listener, statusQueue));
                }
                // now listen for it to finish
                // wait a maximum of a minute
                Boolean b = statusQueue.poll(60, TimeUnit.SECONDS);
                if (b == null) {
                    throw (failureException = new RuntimeException("Scanning timed out"));
                }
            } catch (InterruptedException e) {
                throw (failureException = new RuntimeException(e));
            }
        }
    }

    @Override
    protected void setUp() throws Exception {
        pstDir = FileUtils.createTempDirectory("hub-pstdb", "test");
        prtDir = FileUtils.createTempDirectory("hub-prtdb", "test");
        subDir = FileUtils.createTempDirectory("hub-subdb", "test");
        bktb = new BookKeeperTestBase(1);
        bktb.setUp();
        final int port = PortManager.nextFreePort();
        final int sslPort = PortManager.nextFreePort();
        ServerConfiguration conf = new ServerConfiguration() {

            @Override
            public int getServerPort() {
                return port;
            }

            @Override
            public int getSSLServerPort() {
                return sslPort;
            }

            @Override
            public String getZkHost() {
                return bktb.getZkHostPort();
            }

            @Override
            public boolean isStandalone() {
                return false;
            }

            @Override
            public boolean isSSLEnabled() {
                return false;
            }

            @Override
            public int getBkEnsembleSize() {
                return 1;
            }

            @Override
            public int getBkWriteQuorumSize() {
                return 1;
            }

            @Override
            public int getBkAckQuorumSize() {
                return 1;
            }

            @Override
            public String getLeveldbPersistencePath() {
                return getFilePath(pstDir);
            }

            @Override
            public String getLeveldbPartitionDBPath() {
                return getFilePath(prtDir);
            }

            @Override
            public String getLeveldbSubscriptionDBPath() {
                return getFilePath(subDir);
            }

            private String getFilePath(File dir) {
                if (null == dir) {
                    return null;
                }
                try {
                    return dir.getCanonicalPath().toString();
                } catch (IOException ie) {
                    return null;
                }
            }
        };
        HelixSnitch.initializeCluster(conf, 1, new HedwigSocketAddress(InetAddress.getLocalHost().getHostAddress(),
                port, sslPort));
        server = new TestPubSubServer(conf, new ClientConfiguration(), new LoggingExceptionHandler());
        server.start();
        // Sleep a while wait for partitions are assigned.
        TimeUnit.SECONDS.sleep(5);
        logger.info("Started Helix PubSubServer");
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        if (null != server) {
            server.shutdown();
        }
        deleteDir(pstDir);
        deleteDir(prtDir);
        deleteDir(subDir);
        if (null != bktb) {
            bktb.tearDown();
        }
    }

    private void deleteDir(File dir) throws Exception {
        if (null != dir) {
            org.apache.commons.io.FileUtils.deleteDirectory(dir);
        }
    }

    private ByteString getTopicName(int number) {
        return ByteString.copyFromUtf8("topic-" + number);
    }

    @Test(timeout = 60000)
    public void testPersistenceManager() throws Exception {
        List<Thread> publisherThreads = new LinkedList<Thread>();
        List<Thread> scannerThreads = new LinkedList<Thread>();
        Thread thread;
        Semaphore latch = new Semaphore(1);
        logger.info("Acquire topics");
        for (int i = 0; i < NUM_TOPICS_TO_TEST; i++) {
            ByteString topic = getTopicName(i);
            latch.acquire();
            server.getSnitchSeeker().getSnitch(topic).getTopicManager()
                    .getOwner(topic, true, new Callback<HedwigSocketAddress>() {
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    failureException = new RuntimeException(exception);
                    ((Semaphore) ctx).release();
                }

                @Override
                        public void operationFinished(Object ctx, HedwigSocketAddress res) {
                    ((Semaphore) ctx).release();
                }
            } , latch);
            latch.acquire();
            latch.release();
            if (failureException != null) {
                throw (Exception) failureException.getCause();
            }
        }
        logger.info("Started publish messages thru persistence manager.");
        for (int i = 0; i < NUM_TOPICS_TO_TEST; i++) {
            ByteString topic = getTopicName(i);
            List<Message> msgs = HelperMethods.getRandomPublishedMessages(NUM_MESSAGES_TO_TEST, 1024);
            thread = new Thread(new Publisher(topic, msgs));
            publisherThreads.add(thread);
            thread.start();
            thread = new Thread(new ScanVerifier(topic, msgs));
            scannerThreads.add(thread);
        }
        for (Thread t : publisherThreads) {
            t.join();
        }
        logger.info("Publish done");
        for (Thread t : scannerThreads) {
            t.start();
        }
        for (Thread t : scannerThreads) {
            t.join();
        }
        assertEquals(null, failureException);
        for (int i = 0; i < NUM_TOPICS_TO_TEST; i++) {
            ByteString topic = getTopicName(i);
            assertEquals(
                    server.getSnitchSeeker().getSnitch(topic).getPersistenceManager()
                            .getCurrentSeqIdForTopic(getTopicName(i)).getLocalComponent(), NUM_MESSAGES_TO_TEST);
        }
    }

}
