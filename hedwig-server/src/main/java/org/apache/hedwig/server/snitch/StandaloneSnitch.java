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

import java.io.IOException;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.persistence.LeveldbPersistenceManager;
import org.apache.hedwig.server.persistence.LocalDBPersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManagerWithRangeScan;
import org.apache.hedwig.server.persistence.ReadAheadCache;
import org.apache.hedwig.server.subscriptions.InMemorySubscriptionManager;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TrivialOwnAllTopicManager;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;

public class StandaloneSnitch extends AbstractSnitch {

    public StandaloneSnitch(ServerConfiguration conf, ClientConfiguration clientConf,
            ClientSocketChannelFactory clientChannelFactory) throws IOException {
        super(conf, clientConf, clientChannelFactory);
    }

    @Override
    protected TopicManager instantiateTopicManager(OrderedSafeExecutor scheduler) throws IOException {
        return new TrivialOwnAllTopicManager(conf, scheduler);
    }

    @Override
    protected PersistenceManager instantiatePersistenceManager(TopicManager topicMgr, OrderedSafeExecutor scheduler)
            throws IOException {
        PersistenceManagerWithRangeScan underlyingPM;
        if (conf.isLeveldbPersistenceEnabled()) {
            underlyingPM = new LeveldbPersistenceManager(conf, topicMgr, scheduler);
        } else {
            underlyingPM = LocalDBPersistenceManager.instance();
        }

        PersistenceManager pm;
        if (conf.getReadAheadEnabled()) {
            pm = new ReadAheadCache(underlyingPM, conf).start();
        } else {
            pm = underlyingPM;
        }
        return pm;
    }

    @Override
    protected SubscriptionManager instantiateSubscriptionManager(TopicManager tm, PersistenceManager pm,
            DeliveryManager dm, OrderedSafeExecutor scheduler) throws IOException {
        return new InMemorySubscriptionManager(conf, tm, pm, dm, scheduler);
    }

}
