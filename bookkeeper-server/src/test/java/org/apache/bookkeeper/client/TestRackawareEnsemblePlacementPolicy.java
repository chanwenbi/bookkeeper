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
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntArrayList;

public class TestRackawareEnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(TestRackawareEnsemblePlacementPolicy.class);

    RackawareEnsemblePlacementPolicy repp;
    Configuration conf = new CompositeConfiguration();

    @Before
    public void setUp() throws Exception {
        StaticDNSResolver.reset();
        StaticDNSResolver.addNodeToRack(new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), 0),
                NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(new BookieSocketAddress("127.0.0.1", 0), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(new BookieSocketAddress("localhost", 0), NetworkTopology.DEFAULT_RACK);
        LOG.info("Set up static DNS Resolver.");
        conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf);
    }

    @After
    public void tearDown() throws Exception {
        repp.uninitalize();
    }

    @Test(timeout = 60000)
    public void testNodeDown() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r2");
        StaticDNSResolver.addNodeToRack(addr4, "/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        ArrayList<BookieSocketAddress> ensemble = new ArrayList<>(addrs);
        IntArrayList writeSet = new IntArrayList(ensemble.size());
        for (int i = 0; i < ensemble.size(); i++) {
            writeSet.add(i);
        }

        addrs.remove(addr1);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        IntArrayList reoderSet = repp.reorderReadSequence(ensemble, writeSet);
        IntArrayList expectedSet = new IntArrayList(writeSet.size());
        expectedSet.add(1);
        expectedSet.add(2);
        expectedSet.add(3);
        expectedSet.add(0);
        LOG.info("reorder set : {}", reoderSet);
        assertFalse(reoderSet == writeSet);
        assertEquals(expectedSet, reoderSet);
    }

    @Test(timeout = 60000)
    public void testNodeReadOnly() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r2");
        StaticDNSResolver.addNodeToRack(addr4, "/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        ArrayList<BookieSocketAddress> ensemble = new ArrayList<>(addrs);
        IntArrayList writeSet = new IntArrayList(ensemble.size());
        for (int i = 0; i < ensemble.size(); i++) {
            writeSet.add(i);
        }

        Set<BookieSocketAddress> readonly = new HashSet<BookieSocketAddress>();
        readonly.add(addr1);
        addrs.remove(addr1);
        repp.onClusterChanged(addrs, readonly);

        IntArrayList reoderSet = repp.reorderReadSequence(ensemble, writeSet);
        IntArrayList expectedSet = new IntArrayList(writeSet.size());
        expectedSet.add(1);
        expectedSet.add(2);
        expectedSet.add(3);
        expectedSet.add(0);
        LOG.info("reorder set : {}", reoderSet);
        assertFalse(reoderSet == writeSet);
        assertEquals(expectedSet, reoderSet);
    }

    @Test(timeout = 60000)
    public void testTwoNodesDown() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r2");
        StaticDNSResolver.addNodeToRack(addr4, "/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        ArrayList<BookieSocketAddress> ensemble = new ArrayList<>(addrs);
        IntArrayList writeSet = new IntArrayList(ensemble.size());
        for (int i = 0; i < ensemble.size(); i++) {
            writeSet.add(i);
        }

        addrs.remove(addr1);
        addrs.remove(addr2);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        IntArrayList reoderSet = repp.reorderReadSequence(ensemble, writeSet);
        IntArrayList expectedSet = new IntArrayList(writeSet.size());
        expectedSet.add(2);
        expectedSet.add(3);
        expectedSet.add(0);
        expectedSet.add(1);
        LOG.info("reorder set : {}", reoderSet);
        assertFalse(reoderSet == writeSet);
        assertEquals(expectedSet, reoderSet);
    }

    @Test(timeout = 60000)
    public void testNodeDownAndReadOnly() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r2");
        StaticDNSResolver.addNodeToRack(addr4, "/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        ArrayList<BookieSocketAddress> ensemble = new ArrayList<>(addrs);
        IntArrayList writeSet = new IntArrayList(ensemble.size());
        for (int i = 0; i < ensemble.size(); i++) {
            writeSet.add(i);
        }

        Set<BookieSocketAddress> readonly = new HashSet<BookieSocketAddress>();
        readonly.add(addr2);
        addrs.remove(addr1);
        addrs.remove(addr2);
        repp.onClusterChanged(addrs, readonly);

        IntArrayList reoderSet = repp.reorderReadSequence(ensemble, writeSet);
        IntArrayList expectedSet = new IntArrayList(writeSet.size());
        expectedSet.add(2);
        expectedSet.add(3);
        expectedSet.add(1);
        expectedSet.add(0);
        assertFalse(reoderSet == writeSet);
        assertEquals(expectedSet, reoderSet);
    }

    @Test(timeout = 60000)
    public void testReplaceBookieWithEnoughBookiesInSameRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1,
                NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r2");
        StaticDNSResolver.addNodeToRack(addr4, "/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        Set<BookieSocketAddress> ensembleBookies = new HashSet<BookieSocketAddress>();
        ensembleBookies.add(addr1);
        ensembleBookies.add(addr2);
        ensembleBookies.add(addr4);
        BookieSocketAddress replacedBookie = repp.replaceBookie(addr2, ensembleBookies,
                new HashSet<BookieSocketAddress>());
        assertEquals(addr3, replacedBookie);
    }

    @Test(timeout = 60000)
    public void testReplaceBookieWithEnoughBookiesInDifferentRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r3");
        StaticDNSResolver.addNodeToRack(addr4, "/r4");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();
        excludedAddrs.add(addr1);
        Set<BookieSocketAddress> ensembleBookies = new HashSet<BookieSocketAddress>();
        ensembleBookies.add(addr4);
        ensembleBookies.add(addr2);
        BookieSocketAddress replacedBookie = repp.replaceBookie(addr2, ensembleBookies, excludedAddrs);

        assertFalse(addr1.equals(replacedBookie));
        assertEquals(addr3, replacedBookie);
    }

    @Test(timeout = 60000)
    public void testReplaceBookieWithNotEnoughBookies() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r3");
        StaticDNSResolver.addNodeToRack(addr4, "/r4");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();
        excludedAddrs.add(addr1);
        excludedAddrs.add(addr3);
        excludedAddrs.add(addr4);
        try {
            repp.replaceBookie(addr2, new HashSet<BookieSocketAddress>(), excludedAddrs);
            fail("Should throw BKNotEnoughBookiesException when there is not enough bookies");
        } catch (BKNotEnoughBookiesException bnebe) {
            // should throw not enou
        }
    }

    @Test(timeout = 60000)
    public void testReplaceBookieWithEnoughBookiesInSameRackAsEnsemble() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r2");
        StaticDNSResolver.addNodeToRack(addr4, "/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        Set<BookieSocketAddress> ensembleBookies = new HashSet<BookieSocketAddress>();
        ensembleBookies.add(addr2);
        ensembleBookies.add(addr4);
        BookieSocketAddress replacedBookie = repp.replaceBookie(addr4, ensembleBookies,
                new HashSet<BookieSocketAddress>());
        assertEquals(addr1, replacedBookie);
    }

    @Test(timeout = 60000)
    public void testNewEnsembleWithSingleRack() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(3, 2, new HashSet<BookieSocketAddress>());
            assertEquals(0, getNumCoveredWriteQuorums(ensemble, 2));
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, new HashSet<BookieSocketAddress>());
            assertEquals(0, getNumCoveredWriteQuorums(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test(timeout = 60000)
    public void testNewEnsembleWithMultipleRacks() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r2");
        StaticDNSResolver.addNodeToRack(addr4, "/r2");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(3, 2, new HashSet<BookieSocketAddress>());
            int numCovered = getNumCoveredWriteQuorums(ensemble, 2);
            assertTrue(numCovered >= 1 && numCovered < 3);
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, new HashSet<BookieSocketAddress>());
            numCovered = getNumCoveredWriteQuorums(ensemble2, 2);
            assertTrue(numCovered >= 1 && numCovered < 3);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test(timeout = 90000)
    public void testNewEnsembleWithEnoughRacks() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.6", 3181);
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.7", 3181);
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.8", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r3");
        StaticDNSResolver.addNodeToRack(addr4, "/r4");
        StaticDNSResolver.addNodeToRack(addr5, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr6, "/r2");
        StaticDNSResolver.addNodeToRack(addr7, "/r3");
        StaticDNSResolver.addNodeToRack(addr8, "/r4");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            ArrayList<BookieSocketAddress> ensemble1 = repp.newEnsemble(3, 2, new HashSet<BookieSocketAddress>());
            assertEquals(3, getNumCoveredWriteQuorums(ensemble1, 2));
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, new HashSet<BookieSocketAddress>());
            assertEquals(4, getNumCoveredWriteQuorums(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    /**
     * Test for BOOKKEEPER-633
     */
    @Test(timeout = 60000)
    public void testRemoveBookieFromCluster() {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r2");
        StaticDNSResolver.addNodeToRack(addr4, "/r3");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        addrs.remove(addr1);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
    }

    @Test(timeout = 60000)
    public void testNotifyRackChange() throws Exception {
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1, NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2, "/r2");
        StaticDNSResolver.addNodeToRack(addr3, "/r2");
        StaticDNSResolver.addNodeToRack(addr4, "/r2");
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());

        ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(3, 2, new HashSet<BookieSocketAddress>());
        int numCovered = getNumCoveredWriteQuorums(ensemble, 2);
        assertTrue(numCovered >= 1 && numCovered < 3);

        List<BookieSocketAddress> bookieAddressList = new ArrayList<>();
        List<String> rackList = new ArrayList<>();
        bookieAddressList.add(addr2);
        rackList.add("/r3");
        StaticDNSResolver.changeRack(bookieAddressList, rackList);

        ensemble = repp.newEnsemble(3, 2, new HashSet<BookieSocketAddress>());
        assertEquals(3, getNumCoveredWriteQuorums(ensemble, 2));
    }

    private int getNumCoveredWriteQuorums(ArrayList<BookieSocketAddress> ensemble, int writeQuorumSize)
            throws Exception {
        int ensembleSize = ensemble.size();
        int numCoveredWriteQuorums = 0;
        for (int i = 0; i < ensembleSize; i++) {
            Set<String> racks = new HashSet<String>();
            for (int j = 0; j < writeQuorumSize; j++) {
                int bookieIdx = (i + j) % ensembleSize;
                BookieSocketAddress addr = ensemble.get(bookieIdx);
                racks.add(StaticDNSResolver.getRack(addr));
            }
            numCoveredWriteQuorums += (racks.size() > 1 ? 1 : 0);
        }
        return numCoveredWriteQuorums;
    }
}
