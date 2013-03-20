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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.hedwig.HelperMethods;
import org.apache.hedwig.StubCallback;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PartitionData;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.TestPersistenceManagerBlackBox;
import org.apache.hedwig.server.topics.TopicOwnershipChangeListener;
import org.apache.hedwig.server.topics.TrivialOwnAllTopicManager;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.Either;
import org.apache.hedwig.util.FileUtils;

import com.google.protobuf.ByteString;

public class TestHelixLocalPersistenceStorage extends TestPersistenceManagerBlackBox {

    File persistencedbDir;
    File partitiondbDir;
    HelixLocalPersistenceStorage store;

    class LocalPublisher implements Runnable {
        List<Message> pubMsgs;
        ByteString topic;

        public LocalPublisher(ByteString topic, List<Message> pubMsgs) {
            this.pubMsgs = pubMsgs;
            this.topic = topic;
        }

        @Override
        public void run() {
            LinkedBlockingQueue<Boolean> statusQueue = new LinkedBlockingQueue<Boolean>();
            long seqid = 1;
            for (Message msg : pubMsgs) {
                MessageSeqId msgId = MessageSeqId.newBuilder().setLocalComponent(seqid).build();
                Message msgToPersist = Message.newBuilder(msg).setMsgId(msgId).build();
                ++seqid;
                try {
                    store.persistMessage(topic, msgId, msgToPersist, testCallback, statusQueue);
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

    @Override
    protected void setUp() throws Exception {
        persistencedbDir = FileUtils.createTempDirectory("hub-persistencedb", "test");
        partitiondbDir = FileUtils.createTempDirectory("hub-partitiondb", "test");
        ServerConfiguration conf = new ServerConfiguration() {

            @Override
            public String getLeveldbPersistencePath() {
                return getFilePath(persistencedbDir);
            }

            @Override
            public String getLeveldbPartitionDBPath() {
                return getFilePath(partitiondbDir);
            }

            private String getFilePath(File dir) {
                try {
                    return dir.getCanonicalPath().toString();
                } catch (IOException e) {
                    return null;
                }
            }

        };

        OrderedSafeExecutor scheduler = new OrderedSafeExecutor(conf.getNumTopicQueuerThreads());
        store = new HelixLocalPersistenceStorage(conf, new TrivialOwnAllTopicManager(conf, scheduler), scheduler);
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        if (null != persistencedbDir) {
            org.apache.commons.io.FileUtils.deleteDirectory(persistencedbDir);
        }
        if (null != partitiondbDir) {
            org.apache.commons.io.FileUtils.deleteDirectory(partitiondbDir);
        }
        super.tearDown();
    }

    @Override
    protected long getLowestSeqId() {
        return 1;
    }

    @Override
    protected PersistenceManager instantiatePersistenceManager() {
        return store;
    }

    @Override
    protected long getExpectedSeqId(int numPublished) {
        return numPublished;
    }

    public static Test suite() {
        return new TestSuite(TestHelixLocalPersistenceStorage.class);
    }

    @org.junit.Test(timeout = 60000)
    public void testReadWritePartitionData() throws Exception {
        String partitionName = "testPartition";
        StubCallback<PartitionData> dataCallback = new StubCallback<PartitionData>();
        store.readPartitionData(partitionName, dataCallback, null);
        Either<PartitionData, PubSubException> readRes = dataCallback.queue.take();
        Assert.assertEquals("Found inconsistent partition data", null, readRes.left());
        Assert.assertEquals("Should not fail with PubSubException", null, readRes.right());
        StubCallback<Void> writeCallback = new StubCallback<Void>();
        PartitionData partitionData = PartitionData.newBuilder().setEpoch(999).build();
        store.updatePartitionData(partitionName, partitionData, writeCallback, null);
        Assert.assertNull("Should not fail to update a non-existed subscriber with PubSubException",
                writeCallback.queue.take().right());
        store.readPartitionData(partitionName, dataCallback, null);
        readRes = dataCallback.queue.take();
        Assert.assertEquals("Should not fail with PubSubException", null, readRes.right());
        Assert.assertEquals("Found inconsistent partition data", partitionData.getEpoch(), readRes.left().getEpoch());
    }

    @org.junit.Test(timeout = 60000)
    public void testPersistLocalMessages() throws Exception {
        List<Thread> publisherThreads = new LinkedList<Thread>();
        List<Thread> scannerThreads = new LinkedList<Thread>();
        Thread thread;
        Semaphore latch = new Semaphore(1);

        for (int i = 0; i < NUM_TOPICS_TO_TEST; i++) {
            ByteString topic = getTopicName(i);
            if (persistenceManager instanceof TopicOwnershipChangeListener) {
                TopicOwnershipChangeListener tocl = (TopicOwnershipChangeListener) persistenceManager;
                latch.acquire();
                tocl.acquiredTopic(topic, new Callback<Void>() {
                    @Override
                    public void operationFailed(Object ctx, PubSubException exception) {
                        failureException = new RuntimeException(exception);
                        ((Semaphore) ctx).release();
                    }

                    @Override
                    public void operationFinished(Object ctx, Void res) {
                        ((Semaphore) ctx).release();
                    }
                }, latch);

                latch.acquire();
                latch.release();
                if (failureException != null) {
                    throw (Exception) failureException.getCause();
                }
            }
            List<Message> msgs = HelperMethods.getRandomPublishedMessages(NUM_MESSAGES_TO_TEST, 1024);

            thread = new Thread(new LocalPublisher(topic, msgs));
            publisherThreads.add(thread);
            thread.start();

            thread = new Thread(new ScanVerifier(topic, msgs));
            scannerThreads.add(thread);
        }
        for (Thread t : publisherThreads) {
            t.join();
        }

        for (Thread t : scannerThreads) {
            t.start();
        }

        for (Thread t : scannerThreads) {
            t.join();
        }

        assertEquals(null, failureException);
        for (int i = 0; i < NUM_TOPICS_TO_TEST; i++) {
            assertEquals(getExpectedSeqId(NUM_MESSAGES_TO_TEST), store.getCurrentSeqIdForTopic(getTopicName(i))
                    .getLocalComponent());
        }

    }

}
