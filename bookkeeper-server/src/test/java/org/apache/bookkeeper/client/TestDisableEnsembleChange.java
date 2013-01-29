/*
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

package org.apache.bookkeeper.client;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.net.InetSocketAddress;
import org.junit.Test;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;

public class TestDisableEnsembleChange extends BookKeeperClusterTestCase {

    static Logger LOG = LoggerFactory.getLogger(TestDisableEnsembleChange.class);

    public TestDisableEnsembleChange() {
        super(4);
    }

    @Test(timeout=20000)
    public void testDisableEnsembleChange() throws Exception {
        disableEnsembleChangeTest(true);
    }

    @Test(timeout=20000)
    public void testDisableEnsembleChangeNotEnoughBookies() throws Exception {
        disableEnsembleChangeTest(false);
    }

    void disableEnsembleChangeTest(boolean startNewBookie) throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zkUtil.getZooKeeperConnectString());

        BookKeeper bkc = new BookKeeper(conf);
        // disable ensemble change
        bkc.disableEnsembleChange();

        final byte[] pwd = new byte[0];
        final LedgerHandle lh = bkc.createLedger(4, 3, 2, BookKeeper.DigestType.CRC32, pwd);
        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicBoolean failTest = new AtomicBoolean(false);
        final byte[] entry = "testDisableEnsembleChange".getBytes();

        // record the ensemble mapping before any adds
        assertEquals(1, lh.getLedgerMetadata().getEnsembles().size());
        ArrayList<InetSocketAddress> ensemblesBeforeFailure =
            new ArrayList<InetSocketAddress>(lh.getLedgerMetadata().getEnsembles()
                                             .entrySet().iterator().next().getValue());

        Thread addThread = new Thread() {
            @Override
            public void run() {
                try {
                    while (!finished.get()) {
                        lh.addEntry(entry);
                    }
                } catch (Exception e) {
                    LOG.error("Exception in add entry thread", e);
                    failTest.set(true);
                }
            }
        };
        addThread.start();
        Thread.sleep(2000);
        // kill a bookie to enter handle bookie failure logic
        // but it was expected that there is no ensemble changed.
        killBookie(0);
        Thread.sleep(4000);
        finished.set(true);
        addThread.join();

        assertFalse("Should not fail adding entries facing one bookie failure when disable ensemble change.",
                    failTest.get());

        // check the ensemble after failure
        assertEquals("No new ensemble should not be added when disable ensemble change.",
                     1, lh.getLedgerMetadata().getEnsembles().size());
        ArrayList<InetSocketAddress> ensemblesAfterFailure =
            new ArrayList<InetSocketAddress>(lh.getLedgerMetadata().getEnsembles()
                                             .entrySet().iterator().next().getValue());
        assertEquals(ensemblesBeforeFailure.size(), ensemblesAfterFailure.size());
        for (int i=0; i<ensemblesAfterFailure.size(); i++) {
            assertEquals("No bookie should be replaced when disable ensemble change.",
                         ensemblesBeforeFailure.get(i), ensemblesAfterFailure.get(i));
        }

        if (startNewBookie) {
            startNewBookie();
        }
        // enable ensemble change
        bkc.enableEnsembleChange();
        // reset add thread state
        finished.set(false);
        final CountDownLatch failLatch = new CountDownLatch(1);
        addThread = new Thread() {
            @Override
            public void run() {
                try {
                    while (!finished.get()) {
                        lh.addEntry(entry);
                    }
                } catch (Exception e) {
                    LOG.error("Exception in add entry thread", e);
                    failLatch.countDown();
                    failTest.set(true);
                }
            }
        };
        addThread.start();
        failLatch.await(4000, TimeUnit.MILLISECONDS);
        finished.set(true);
        addThread.join();

        if (startNewBookie) {
            assertFalse("Should not fail adding entries when enable ensemble change again.",
                        failTest.get());
            assertFalse("Ledger should be closed when enable ensemble change again.",
                        lh.getLedgerMetadata().isClosed()); 
            assertEquals("New ensemble should be added when enable ensemble change again.",
                         2, lh.getLedgerMetadata().getEnsembles().size());
        } else {
            assertTrue("Should fail adding entries when enable ensemble change again.",
                       failTest.get());
            assertTrue("Ledger should be closed when enable ensemble change again.",
                       lh.getLedgerMetadata().isClosed()); 
        }
    }

    @Test(timeout=20000)
    public void testRetryFailureBookie() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zkUtil.getZooKeeperConnectString());

        BookKeeper bkc = new BookKeeper(conf);
        // disable ensemble change
        bkc.disableEnsembleChange();

        LedgerHandle lh = bkc.createLedger(4, 4, 4, BookKeeper.DigestType.CRC32, new byte[] {});
        byte[] entry = "testRetryFailureBookie".getBytes();
        for (int i=0; i<10; i++) {
            lh.addEntry(entry);
        }
        // kill a bookie
        ServerConfiguration killedConf = killBookie(0);

        final AtomicInteger res = new AtomicInteger(0xdeadbeef);
        final CountDownLatch addLatch = new CountDownLatch(1);
        AsyncCallback.AddCallback cb = new AsyncCallback.AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                LOG.info("Add entry {} completed : rc {}.", entryId, rc);
                res.set(rc);
                addLatch.countDown();
            }
        };
        lh.asyncAddEntry(entry, cb, null);
        assertFalse("Add entry operation should not complete.",
                    addLatch.await(1000, TimeUnit.MILLISECONDS));
        assertEquals(res.get(), 0xdeadbeef);
        // start the original bookie
        bsConfs.add(killedConf);
        bs.add(startBookie(killedConf));
        assertTrue("Add entry operation should complete at this point.",
                   addLatch.await(1000, TimeUnit.MILLISECONDS));
        assertEquals(res.get(), BKException.Code.OK);
    }

    @Test(timeout=20000)
    public void testRetrySlowBookie() throws Exception {
        final int readTimeout = 2;

        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zkUtil.getZooKeeperConnectString())
            .setReadTimeout(readTimeout);

        BookKeeper bkc = new BookKeeper(conf);
        // disable ensemble change
        bkc.disableEnsembleChange();

        LedgerHandle lh = bkc.createLedger(4, 4, 4, BookKeeper.DigestType.CRC32, new byte[] {});
        byte[] entry = "testRetryFailureBookie".getBytes();
        for (int i=0; i<10; i++) {
            lh.addEntry(entry);
        }

        List<InetSocketAddress> curEns = lh.getLedgerMetadata().currentEnsemble;

        final CountDownLatch wakeupLatch = new CountDownLatch(1);
        sleepBookie(curEns.get(2), wakeupLatch);

        final AtomicInteger res = new AtomicInteger(0xdeadbeef);
        final CountDownLatch addLatch = new CountDownLatch(1);
        AsyncCallback.AddCallback cb = new AsyncCallback.AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                LOG.info("Add entry {} completed : rc {}.", entryId, rc);
                res.set(rc);
                addLatch.countDown();
            }
        };
        lh.asyncAddEntry(entry, cb, null);
        assertFalse("Add entry operation should not complete.",
                    addLatch.await(1000, TimeUnit.MILLISECONDS));
        assertEquals(res.get(), 0xdeadbeef);
        // wait until read timeout
        assertFalse("Add entry operation should not complete even timeout.",
                    addLatch.await(readTimeout, TimeUnit.SECONDS));
        assertEquals(res.get(), 0xdeadbeef);
        // wait one more read timeout, to ensure we resend multiple retries
        // to ensure it works correctly even with problem in BOOKKEEPER-49
        assertFalse("Add entry operation should not complete even timeout.",
                    addLatch.await(readTimeout, TimeUnit.SECONDS));
        assertEquals(res.get(), 0xdeadbeef);
        // wakeup the sleep bookie
        wakeupLatch.countDown();
        assertTrue("Add entry operation should complete at this point.",
                   addLatch.await(1000, TimeUnit.MILLISECONDS));
        assertEquals(res.get(), BKException.Code.OK);
    }
}
