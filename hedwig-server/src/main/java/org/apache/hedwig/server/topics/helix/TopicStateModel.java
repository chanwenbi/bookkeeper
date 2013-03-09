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

import static org.apache.hedwig.server.topics.helix.States.LEADER;
import static org.apache.hedwig.server.topics.helix.States.OFFLINE;
import static org.apache.hedwig.server.topics.helix.States.STANDBY;
import static org.apache.hedwig.util.VarArgs.va;

import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StateModelInfo(initialState = OFFLINE, states = { LEADER, STANDBY })
public class TopicStateModel extends StateModel {

    private static final Logger logger = LoggerFactory.getLogger(TopicStateModel.class);

    private static final String FIELD_EPOCH = "epoch";
    private static final long INITIAL_EPOCH = 0L;

    private final String instanceName;
    private final String partitionName;
    private final TopicPartitionOwnershipListener listener;

    public TopicStateModel(String instanceName, String partition, TopicPartitionOwnershipListener listener) {
        this.instanceName = instanceName;
        this.partitionName = partition;
        this.listener = listener;
    }

    private static void logStateTransition(String instanceName, String partitionName, String from, String to) {
        logger.info("Instance {} : TopicPartition {} become {} from {}.", va(instanceName, partitionName, to, from));
    }

    @Transition(to = STANDBY, from = OFFLINE)
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
        logStateTransition(instanceName, partitionName, OFFLINE, STANDBY);
        listener.becomeStandbyFromOffline(partitionName);
    }

    @Transition(to = LEADER, from = STANDBY)
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
        logStateTransition(instanceName, partitionName, STANDBY, LEADER);
        // claim the topic partition when it becomes leader.
        ConfigScopeBuilder builder = new ConfigScopeBuilder();
        builder.forCluster(HelixTopicManager.DEFAULT_CLUSTERNAME).forResource(HelixTopicManager.DEFAULT_NAMESPACE)
                .forPartition(partitionName);
        ConfigScope scope = builder.build();
        logger.info("{} : STANDBY -> LEADER : Access scope {} for epoch of partition {}.",
                va(instanceName, scope, partitionName));
        String epochStr = context.getManager().getConfigAccessor().get(scope, FIELD_EPOCH);
        long epoch = INITIAL_EPOCH;
        if (null != epochStr) {
            try {
                epoch = Long.parseLong(epochStr) + 1;
            } catch (NumberFormatException nfe) {
            }
        }
        context.getManager().getConfigAccessor().set(scope, FIELD_EPOCH, "" + epoch);
        logger.info("{} : STANDBY -> LEADER : Bumping epoch to {} for partition {}.",
                va(instanceName, epoch, partitionName));
        listener.becomeLeaderFromStandby(partitionName, epoch);
    }

    @Transition(to = STANDBY, from = LEADER)
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
        logStateTransition(instanceName, partitionName, LEADER, STANDBY);
        // release the topic partition when it becomes standby
        listener.becomeStandbyFromLeader(partitionName);
    }

    @Transition(to = OFFLINE, from = STANDBY)
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
        logStateTransition(instanceName, partitionName, STANDBY, OFFLINE);
        listener.becomeOfflineFromStandby(partitionName);
    }

}