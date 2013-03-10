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

import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.handlers.SubscriptionChannelManager.SubChannelDisconnectedListener;
import org.apache.hedwig.server.jmx.HedwigMBeanInfo;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.topics.TopicManager;

/**
 * A snitch referred to as simply the <i>Gold Snitch</i>, which is the ball in
 * Guidditch of Harry Potter.
 *
 * A <i>Snitch</i> here is a container including all the components we used to
 * serve pub/sub message delivery. These components includes:
 * {@link TopicManager}, {@link PersistenceManager}, {@link SubscriptionManager}
 * and {@link DeliveryManager}. {@link RegionManager} isn't included as a
 * required component, since some snitch implementations doesn't require a
 * cross-region functionality.
 */
public interface Snitch {

    /**
     * Start a <i>snitch<i>
     */
    public void start();

    /**
     * Stop the <i>snitch<i>
     */
    public void stop();

    /**
     * Get the {@link TopicManager} used in this <i>snitch</i> to serve topics
     * ownerships.
     *
     * @return topic manager used in this <i>snitch</i>.
     */
    public TopicManager getTopicManager();

    /**
     * Get the {@link PersistenceManager} used in this <i>snitch</i> to persist
     * all published messages.
     *
     * @return persistence manager used in this <i>snitch</i>.
     */
    public PersistenceManager getPersistenceManager();

    /**
     * Get the {@link SubscriptionManager} used in this <i>snitch</i> to store
     * subscriptions for their topics.
     *
     * @return subscription manager used in this <i>snitch</i>.
     */
    public SubscriptionManager getSubscriptionManager();

    /**
     * Get the {@link DeliveryManager} used in this <i>snitch</i> to deliver
     * messages.
     *
     * @return delivery manager used in this <i>snitch</i>.
     */
    public DeliveryManager getDeliveryManager();

    /**
     * Return the subscription channel disconnected listener.
     *
     * @return the subscription channel disconnected listener.
     */
    public SubChannelDisconnectedListener getSubChannelDisconnectedListener();

    /**
     * Register JMX.
     *
     * @param parent
     *            Parent JMX Bean
     */
    public void registerJMX(HedwigMBeanInfo parent);

    /**
     * Unregister JMX.
     */
    public void unregisterJMX();

}
