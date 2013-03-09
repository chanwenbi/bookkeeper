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

import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.server.handlers.SubscriptionChannelManager.SubChannelDisconnectedListener;
import org.apache.hedwig.server.snitch.Snitch;
import org.apache.hedwig.server.snitch.SnitchSeeker;

import com.google.protobuf.ByteString;

public class HelixSnitchSeeker implements SnitchSeeker {

    final static byte LOG_TOPIC_FLAG = '.';

    final Snitch logSnitch;
    final Snitch topicSnitch;
    final SubChannelDisconnectedListener logSnitchListener;
    final SubChannelDisconnectedListener topicSnitchListener;

    public HelixSnitchSeeker(final Snitch topicSnitch, final Snitch logSnitch) {
        this.topicSnitch = topicSnitch;
        this.topicSnitchListener = topicSnitch.getSubChannelDisconnectedListener();
        this.logSnitch = logSnitch;
        this.logSnitchListener = logSnitch.getSubChannelDisconnectedListener();
    }

    private boolean isALogTopic(ByteString topic) {
        return topic.byteAt(0) == LOG_TOPIC_FLAG;
    }

    @Override
    public void onSubChannelDisconnected(TopicSubscriber topicSubscriber) {
        if (isALogTopic(topicSubscriber.getTopic())) {
            logSnitchListener.onSubChannelDisconnected(topicSubscriber);
        } else {
            topicSnitchListener.onSubChannelDisconnected(topicSubscriber);
        }
    }

    @Override
    public Snitch getSnitch(ByteString topic) {
        if (isALogTopic(topic)) {
            return logSnitch;
        } else {
            return topicSnitch;
        }
    }

    @Override
    public void start() {
        logSnitch.start();
        topicSnitch.start();
    }

    @Override
    public void stop() {
        topicSnitch.stop();
        logSnitch.stop();
    }

    @Override
    public void postStart() {
        logSnitch.postStart();
        topicSnitch.postStart();
    }

}
