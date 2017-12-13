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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a synchronization thread.
 */
public class TestSyncThread {
    private static final Logger LOG = LoggerFactory.getLogger(TestSyncThread.class);

    ExecutorService executor = null;

    @Before
    public void setupExecutor() {
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void teardownExecutor() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    /**
     * Test that if a flush is taking a long time,
     * the sync thread will not shutdown until it
     * has finished.
     */
    @Test
    public void testSyncThreadLongShutdown() throws Exception {
        int flushInterval = 100;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setFlushInterval(flushInterval);
        CheckpointSource checkpointSource = newDummyCheckpointSource();
        LedgerDirsListener listener = mock(LedgerDirsListener.class);

        final CountDownLatch checkpointCalledLatch = new CountDownLatch(1);
        final CountDownLatch checkpointLatch = new CountDownLatch(1);

        final CountDownLatch flushCalledLatch = new CountDownLatch(1);
        final CountDownLatch flushLatch = new CountDownLatch(1);
        final AtomicBoolean failedSomewhere = new AtomicBoolean(false);

        LedgerStorage storage = mock(LedgerStorage.class);
        // mock flush method
        doAnswer(invocationOnMock -> {
            flushCalledLatch.countDown();
            try {
                flushLatch.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted in flush thread", ie);
                failedSomewhere.set(true);
            }
            return null;
        }).when(storage).flush();
        // mock checkpoint method
        doAnswer(invocationOnMock -> {
            checkpointCalledLatch.countDown();
            try {
                checkpointLatch.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted in checkpoint thread", ie);
                failedSomewhere.set(true);
            }
            return null;
        }).when(storage).checkpoint(any(Checkpoint.class));

        final SyncThread t = new SyncThread(conf, listener, storage, checkpointSource);
        t.startCheckpoint(Checkpoint.MAX);
        assertTrue("Checkpoint should have been called",
                   checkpointCalledLatch.await(10, TimeUnit.SECONDS));
        Future<Boolean> done = executor.submit(() -> {
            try {
                t.shutdown();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted shutting down sync thread", ie);
                failedSomewhere.set(true);
                return false;
            }
            return true;
        });
        checkpointLatch.countDown();
        assertFalse("Shutdown shouldn't have finished", done.isDone());
        assertTrue("Flush should have been called",
                   flushCalledLatch.await(10, TimeUnit.SECONDS));

        assertFalse("Shutdown shouldn't have finished", done.isDone());
        flushLatch.countDown();

        assertTrue("Shutdown should have finished successfully", done.get(10, TimeUnit.SECONDS));
        assertFalse("Shouldn't have failed anywhere", failedSomewhere.get());
    }

    /**
     * Test that sync thread suspension works.
     * i.e. when we suspend the syncthread, nothing
     * will be synced.
     */
    @Test
    public void testSyncThreadSuspension() throws Exception {
        int flushInterval = 100;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setFlushInterval(flushInterval);
        CheckpointSource checkpointSource = newDummyCheckpointSource();
        LedgerDirsListener listener = mock(LedgerDirsListener.class);

        final AtomicInteger checkpointCount = new AtomicInteger(0);
        LedgerStorage storage = mock(LedgerStorage.class);
        doAnswer(invocationOnMock -> {
            checkpointCount.incrementAndGet();
            return null;
        }).when(storage).checkpoint(any(Checkpoint.class));
        final SyncThread t = new SyncThread(conf, listener, storage, checkpointSource);
        t.startCheckpoint(Checkpoint.MAX);
        while (checkpointCount.get() == 0) {
            Thread.sleep(flushInterval);
        }
        t.suspendSync();
        Thread.sleep(flushInterval);
        int count = checkpointCount.get();
        for (int i = 0; i < 10; i++) {
            t.startCheckpoint(Checkpoint.MAX);
            assertEquals("Checkpoint count shouldn't change", count, checkpointCount.get());
        }
        t.resumeSync();
        int i = 0;
        while (checkpointCount.get() == count) {
            Thread.sleep(flushInterval);
            i++;
            if (i > 100) {
                fail("Checkpointing never resumed");
            }
        }
        t.shutdown();
    }

    /**
     * Test that if the ledger storage throws a
     * runtime exception, the bookie will be told
     * to shutdown.
     */
    @Test
    public void testSyncThreadShutdownOnError() throws Exception {
        int flushInterval = 100;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setFlushInterval(flushInterval);
        CheckpointSource checkpointSource = newDummyCheckpointSource();
        final CountDownLatch fatalLatch = new CountDownLatch(1);
        LedgerDirsListener listener = mock(LedgerDirsListener.class);
        doAnswer(invocationOnMock -> {
            fatalLatch.countDown();
            return null;
        }).when(listener).fatalError();

        LedgerStorage storage = mock(LedgerStorage.class);
        doThrow(new RuntimeException("Fatal error in sync thread"))
            .when(storage).checkpoint(any(Checkpoint.class));
        final SyncThread t = new SyncThread(conf, listener, storage, checkpointSource);
        t.startCheckpoint(Checkpoint.MAX);
        assertTrue("Should have called fatal error", fatalLatch.await(10, TimeUnit.SECONDS));
        t.shutdown();
    }

    /**
     * Test that if the ledger storage throws
     * a disk full exception, the owner of the sync
     * thread will be notified.
     */
    @Test
    public void testSyncThreadDisksFull() throws Exception {
        int flushInterval = 100;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setFlushInterval(flushInterval);
        CheckpointSource checkpointSource = newDummyCheckpointSource();
        final CountDownLatch diskFullLatch = new CountDownLatch(1);
        LedgerDirsListener listener = mock(LedgerDirsListener.class);
        doAnswer(invocationOnMock -> {
            diskFullLatch.countDown();
            return null;
        }).when(listener).allDisksFull();

        LedgerStorage storage = mock(LedgerStorage.class);
        doThrow(new NoWritableLedgerDirException("Disk full error in sync thread"))
            .when(storage).checkpoint(any(Checkpoint.class));
        final SyncThread t = new SyncThread(conf, listener, storage, checkpointSource);
        t.startCheckpoint(Checkpoint.MAX);
        assertTrue("Should have disk full error", diskFullLatch.await(10, TimeUnit.SECONDS));
        t.shutdown();
    }

    private static CheckpointSource newDummyCheckpointSource() {
        CheckpointSource source = mock(CheckpointSource.class);
        when(source.newCheckpoint()).thenReturn(Checkpoint.MAX);
        return source;
    }

}
