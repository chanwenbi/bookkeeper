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

import org.apache.helix.model.StateModelDefinition;

class States {

    static final String STATE_MODEL_NAME = "hedwig";

    static final String OFFLINE = "OFFLINE";
    static final String LEADER = "LEADER";
    static final String STANDBY = "STANDBY";

    static StateModelDefinition defineTopicStateModel() {
        StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);
        // Add states and their rank to indicate priority
        builder.addState(LEADER, 1);
        builder.addState(STANDBY, 2);
        builder.addState(OFFLINE);
        // Set the initial state when the node starts
        builder.initialState(OFFLINE);
        // Add transitions between the states.
        builder.addTransition(OFFLINE, STANDBY);
        builder.addTransition(STANDBY, LEADER);
        builder.addTransition(LEADER, STANDBY);
        builder.addTransition(STANDBY, OFFLINE);
        // set constraints on LEADER state
        builder.upperBound(LEADER, 1);
        // dynamic constraint, R means it should be derived based on the
        // replication factor.
        builder.dynamicUpperBound(STANDBY, "R");
        return builder.build();
    }
}
