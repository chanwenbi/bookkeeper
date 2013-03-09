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

import static org.apache.hedwig.util.VarArgs.va;

import java.io.IOException;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManagerWithRangeScan;
import org.apache.hedwig.server.persistence.ReadAheadCache;
import org.apache.hedwig.server.regions.HedwigHubClientFactory;
import org.apache.hedwig.server.snitch.AbstractSnitch;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.helix.HelixTopicManager;
import org.apache.hedwig.server.topics.helix.Partitioner;
import org.apache.hedwig.server.topics.helix.States;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

public class HelixSnitch extends AbstractSnitch {

    static final Logger logger = LoggerFactory.getLogger(HelixSnitch.class);

    // TODO: make partitioner configurable.
    static final int NUM_PARTITIONS = 2;
    static protected final Partitioner<ByteString> partitioner = new Partitioner<ByteString>() {

        final String[] partitionNames = new String[NUM_PARTITIONS];

        @Override
        public String getPartition(ByteString key) {
            int pIdx = Hashing.consistentHash(key.hashCode(), NUM_PARTITIONS);
            if (null == partitionNames[pIdx]) {
                partitionNames[pIdx] = HelixTopicManager.DEFAULT_NAMESPACE + "_" + pIdx;
            }
            return partitionNames[pIdx];
        }

    };

    public static InstanceConfig createInstanceConfig(HedwigSocketAddress addr) {
        InstanceConfig instanceConfig = new InstanceConfig(addr.toString());
        instanceConfig.setHostName(addr.getHostname());
        instanceConfig.setPort("" + addr.getPort());
        instanceConfig.setInstanceEnabled(true);
        return instanceConfig;
    }

    // TODO: move this function into a admin class
    public static HelixManager initializeCluster(ServerConfiguration conf, int numReplicas,
            HedwigSocketAddress... addrs) {
        ZKHelixAdmin admin = new ZKHelixAdmin(conf.getZkHost());
        logger.info("Creating cluster : {}", HelixTopicManager.DEFAULT_CLUSTERNAME);
        admin.addCluster(HelixTopicManager.DEFAULT_CLUSTERNAME, true);
        // Add nodes to the cluster
        logger.info("Adding {} nodes to the cluster.", addrs.length);
        for (HedwigSocketAddress addr : addrs) {
            InstanceConfig instanceConf = createInstanceConfig(addr);
            admin.addInstance(HelixTopicManager.DEFAULT_CLUSTERNAME, instanceConf);
            logger.info("Added node : {}.", instanceConf.getInstanceName());
        }
        // Add a state model
        StateModelDefinition stateModel = States.defineTopicStateModel();
        logger.info("Configuring cluster {} with {} replicas.", HelixTopicManager.DEFAULT_CLUSTERNAME, numReplicas);
        admin.addStateModelDef(HelixTopicManager.DEFAULT_CLUSTERNAME, States.STATE_MODEL_NAME, stateModel);
        admin.addResource(HelixTopicManager.DEFAULT_CLUSTERNAME, HelixTopicManager.DEFAULT_NAMESPACE, NUM_PARTITIONS,
                States.STATE_MODEL_NAME, "AUTO");
        logger.info("Added namespace {} with {} partitions, each partition hosted by {} replicas.",
                va(HelixTopicManager.DEFAULT_NAMESPACE, NUM_PARTITIONS, numReplicas));
        // initialize the cluster
        admin.rebalance(HelixTopicManager.DEFAULT_CLUSTERNAME, HelixTopicManager.DEFAULT_NAMESPACE, numReplicas);
        // TODO: move to a seperated component
        logger.info("Starting Helix Controller.");
        HelixManager manager = HelixControllerMain.startHelixController(conf.getZkHost(),
                HelixTopicManager.DEFAULT_CLUSTERNAME, "localhost_9100", HelixControllerMain.STANDALONE);
        logger.info("Helix Controller is started.");
        return manager;
    }

    protected OrderedSafeExecutor ioPool = null;

    public HelixSnitch(ServerConfiguration conf, ClientConfiguration clientConf,
            ClientSocketChannelFactory clientChannelFactory) throws IOException {
        super(conf, clientConf, clientChannelFactory);
    }

    @Override
    protected void startTopicManager() {
        // we don't start anything here, we start topic manager post
        // initialized.
    }

    @Override
    protected void stopTopicManager() {
        ((HelixTopicManager) tm).stop();
    }

    @Override
    public void postStart() {
        // we only start helix topic manager after we started the pub/sub server
        // since it relies on a client connecting to itself
        // TODO: considering co-locate the owner with its transaction log
        // together to
        // reduce network round-trip.
        try {
            ((HelixTopicManager) tm).start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start helix topic manager : ", e);
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (null != ioPool) {
            ioPool.shutdown();
        }
        // stop the subscriber
        ((HelixTopicManager) tm).getHedwigHubClient().close();
        logger.info("Closed hedwig client for partition transaction logs.");
    }

    @Override
    protected TopicManager instantiateTopicManager(OrderedSafeExecutor scheduler) throws IOException {
        InstanceConfig instanceConfig = createInstanceConfig(conf.getServerAddr());
        ioPool = new OrderedSafeExecutor(conf.getLeveldbNumIOWorkers());
        return new HelixTopicManager(conf, instanceConfig, scheduler, ioPool, new HedwigHubClientFactory(conf,
                clientConf, clientChannelFactory), partitioner);
    }

    @Override
    protected PersistenceManager instantiatePersistenceManager(TopicManager topicMgr, OrderedSafeExecutor scheduler)
            throws IOException {
        PersistenceManagerWithRangeScan underlyingPM = ((HelixTopicManager) tm).getPersistenceManager();
        if (conf.getReadAheadEnabled()) {
            return new ReadAheadCache(underlyingPM, conf).start();
        } else {
            return underlyingPM;
        }
    }

    @Override
    protected SubscriptionManager instantiateSubscriptionManager(TopicManager tm, PersistenceManager pm,
            DeliveryManager dm, OrderedSafeExecutor scheduler) throws IOException {
        return new HelixSubscriptionManager(conf, (HelixTopicManager) tm, pm, dm, scheduler);
    }

}
