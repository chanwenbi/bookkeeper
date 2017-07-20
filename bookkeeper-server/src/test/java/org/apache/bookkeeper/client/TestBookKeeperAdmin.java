/*
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

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;

/**
 * This unit test test bookkeeper admin
 */
public class TestBookKeeperAdmin extends BookKeeperClusterTestCase {

    BookKeeper.DigestType digestType;

    public TestBookKeeperAdmin() {
        super(1);
        this.digestType = BookKeeper.DigestType.CRC32;
    }

    @Test(timeout = 60000)
    public void testOpenLedgerForceRecovery() throws Exception {
        // Create a ledger
        byte[] passwd = "open-ledger-force-recovery".getBytes();
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, passwd);

        final long ledgerId = lh.getId();
        final int numEntries = 30;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("" + i).getBytes(UTF_8));
        }

        lh.close();

        // modify the ledger metadata to simulate a wrong last entry id.
        final CountDownLatch doneLatch = new CountDownLatch(1);
        bkc.ledgerManager.readLedgerMetadata(ledgerId, new BookkeeperInternalCallbacks.GenericCallback<LedgerMetadata>() {
            @Override
            public void operationComplete(int rc, LedgerMetadata metadata) {
                if (BKException.Code.OK != rc) {
                    return;
                }
                // modify the metadata
                metadata.close(0L);
                bkc.ledgerManager.writeLedgerMetadata(ledgerId, metadata,
                        new BookkeeperInternalCallbacks.GenericCallback<Void>() {
                            @Override
                            public void operationComplete(int rc, Void result) {
                                if (BKException.Code.OK != rc) {
                                    return;
                                }
                                doneLatch.countDown();
                            }
                        });
            }
        });

        doneLatch.await();

        LedgerHandle readLh = bkc.openLedger(ledgerId, digestType, passwd);
        assertEquals("Last Entry ID should be zero", 0L, readLh.getLastAddConfirmed());
        readLh.close();

        BookKeeperAdmin bka = new BookKeeperAdmin(bkc);
        LedgerHandle adminLh = bka.openLedger(ledgerId, true);
        assertEquals("Last Entry ID should be fixed to be " + (numEntries - 1), (long) (numEntries - 1), adminLh.getLastAddConfirmed());
        adminLh.close();
    }
}
