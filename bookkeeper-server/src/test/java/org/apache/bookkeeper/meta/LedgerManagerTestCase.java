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

package org.apache.bookkeeper.meta;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.bookkeeper.bookie.CompactableLedgerStorage;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.SnapshotMap;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test case to run over serveral ledger managers.
 */
@RunWith(Parameterized.class)
public abstract class LedgerManagerTestCase extends BookKeeperClusterTestCase {

    protected LedgerManagerFactory ledgerManagerFactory;
    protected LedgerManager ledgerManager = null;
    protected LedgerIdGenerator ledgerIdGenerator = null;
    protected SnapshotMap<Long, Boolean> activeLedgers = null;

    public LedgerManagerTestCase(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        this(lmFactoryCls, 0);
    }

    public LedgerManagerTestCase(Class<? extends LedgerManagerFactory> lmFactoryCls, int numBookies) {
        super(numBookies);
        activeLedgers = new SnapshotMap<Long, Boolean>();
        baseConf.setLedgerManagerFactoryClass(lmFactoryCls);
        baseClientConf.setLedgerManagerFactoryClass(lmFactoryCls);
    }

    public LedgerManager getLedgerManager() {
        if (null == ledgerManager) {
            ledgerManager = ledgerManagerFactory.newLedgerManager();
        }
        return ledgerManager;
    }

    public LedgerIdGenerator getLedgerIdGenerator() throws IOException {
        if (null == ledgerIdGenerator) {
            ledgerIdGenerator = ledgerManagerFactory.newLedgerIdGenerator();
        }
        return ledgerIdGenerator;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { FlatLedgerManagerFactory.class },
            { HierarchicalLedgerManagerFactory.class },
            { LongHierarchicalLedgerManagerFactory.class },
            { MSLedgerManagerFactory.class },
        });
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(baseConf, zkc);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (null != ledgerManager) {
            ledgerManager.close();
        }
        ledgerManagerFactory.uninitialize();
        super.tearDown();
    }

    //
    // Mocked Ledger Storage
    //

    private Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) {
        NavigableMap<Long, Boolean> bkActiveLedgersSnapshot = activeLedgers.snapshot();
        Map<Long, Boolean> subBkActiveLedgers = bkActiveLedgersSnapshot
                .subMap(firstLedgerId, true, lastLedgerId, false);

        return subBkActiveLedgers.keySet();
    }

    protected CompactableLedgerStorage newMockLedgerStorage() throws Exception {
        CompactableLedgerStorage storage = mock(CompactableLedgerStorage.class);
        // mock deleteLedger
        doAnswer(invocationOnMock -> {
            long ledgerId = invocationOnMock.getArgument(0);
            activeLedgers.remove(ledgerId);
            return null;
        }).when(storage).deleteLedger(anyLong());
        // mock getActiveLedgersInRange
        doAnswer(invocationOnMock -> {
            long firstLedgerId = invocationOnMock.getArgument(0);
            long lastLedgerId = invocationOnMock.getArgument(1);

            return getActiveLedgersInRange(firstLedgerId, lastLedgerId);
        }).when(storage).getActiveLedgersInRange(anyLong(), anyLong());
        return storage;
    }

}
