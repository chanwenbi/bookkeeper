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
package org.apache.hedwig.server.snitch;

import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.server.handlers.SubscriptionChannelManager.SubChannelDisconnectedListener;

import com.google.protobuf.ByteString;

/**
 * Only one <i>snitch</i> existed in this system.
 */
public class OneSnitchSeeker implements SnitchSeeker {

    final Snitch snitch;
    final SubChannelDisconnectedListener listener;

    public OneSnitchSeeker(Snitch snitch) {
        this.snitch = snitch;
        this.listener = snitch.getSubChannelDisconnectedListener();
    }

    @Override
    public Snitch getSnitch(ByteString topic) {
        return snitch;
    }

    @Override
    public void onSubChannelDisconnected(TopicSubscriber topicSubscriber) {
        if (null != listener) {
            listener.onSubChannelDisconnected(topicSubscriber);
        }
    }

    @Override
    public void start() {
        snitch.start();
    }

    @Override
    public void stop() {
        snitch.stop();
    }

    @Override
    public void postStart() {
        snitch.postStart();
    }

}
