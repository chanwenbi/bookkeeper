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
package org.apache.hedwig.server.topics.helix;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.TopicOpQueuer;
import org.apache.hedwig.server.regions.HedwigHubClient;
import org.apache.hedwig.server.regions.HedwigHubClientFactory;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TopicOwnershipChangeListener;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.statemachine.StateModelFactory;

import com.google.protobuf.ByteString;

public class HelixTopicManager extends StateModelFactory<TopicStateModel> implements TopicManager,
        TopicPartitionOwnershipListener {

    static final String DEFAULT_NAMESPACE = "hedwig";
    static final String DEFAULT_CLUSTERNAME = "hedwig";

    private final ServerConfiguration conf;
    private final InstanceConfig instanceConf;
    private HelixManager manager;
    private final TopicOpQueuer queue;
    private final HedwigHubClient client;

    /**
     * Topic Partition maintains the topics it already claimed for a partition.
     */
    class TopicPartition {

        Set<ByteString>
    }

    public HelixTopicManager(ServerConfiguration conf, InstanceConfig instanceConf, ScheduledExecutorService scheduler,
            HedwigHubClientFactory hubClientFactory) {
        this.conf = conf;
        this.instanceConf = instanceConf;
        this.queue = new TopicOpQueuer(scheduler);
        // establish a client to local hedwig cluster, we will use local hedwig cluster
        // as a pub/sub commit destination
        this.client = hubClientFactory.create(new HedwigSocketAddress("localhost", conf.getServerPort(), conf
                .getSSLServerPort()));
    }

    InstanceConfig getInstanceConfig() {
        return instanceConf;
    }

    public void start() throws Exception {
        String clusterName = DEFAULT_CLUSTERNAME;
        manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceConf.getInstanceName(),
                InstanceType.PARTICIPANT, conf.getZkHost());
        manager.getStateMachineEngine().registerStateModelFactory(States.STATE_MODEL_NAME, this);
        manager.connect();
    }

    @Override
    public void getOwner(ByteString topic, boolean shouldClaim, Callback<HedwigSocketAddress> cb, Object ctx) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addTopicOwnershipChangeListener(TopicOwnershipChangeListener listener) {
        // TODO Auto-generated method stub

    }

    @Override
    public void releaseTopic(ByteString topic, Callback<Void> cb, Object ctx) {
        // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
        manager.disconnect();
    }

    @Override
    public void becomeTopicPartitionLeader(String topicPartition) {
        //
    }

    @Override
    public void becomeTopicPartitionStandby(String topicPartition) {
    }

    @Override
    public TopicStateModel createNewStateModel(String partition) {
        return new TopicStateModel(instanceConf.getInstanceName(), partition, this);
    }

}
