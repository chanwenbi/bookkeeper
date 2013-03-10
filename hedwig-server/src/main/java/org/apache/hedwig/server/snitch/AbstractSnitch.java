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
import org.apache.hedwig.server.delivery.FIFODeliveryManager;
import org.apache.hedwig.server.handlers.SubscriptionChannelManager.SubChannelDisconnectedListener;
import org.apache.hedwig.server.jmx.HedwigMBeanInfo;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.ReadAheadCache;
import org.apache.hedwig.server.regions.HedwigHubClientFactory;
import org.apache.hedwig.server.regions.RegionManager;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;

public abstract class AbstractSnitch implements Snitch {

    protected final ServerConfiguration conf;
    protected final ClientConfiguration clientConf;

    // Netty related variables
    protected final ClientSocketChannelFactory clientChannelFactory;

    // Manager components that make up a StandaloneSnitch
    protected PersistenceManager pm;
    protected TopicManager tm;
    protected SubscriptionManager sm;
    protected DeliveryManager dm;
    protected RegionManager rm;

    // Listener for Subscription Channels Disconnected Events
    protected SubChannelDisconnectedListener listener = null;

    // we use scheduler to prevent long stack chains from building up in
    // callbacks
    protected OrderedSafeExecutor scheduler;

    protected AbstractSnitch(ServerConfiguration conf, ClientConfiguration clientConf,
            ClientSocketChannelFactory clientChannelFactory) throws IOException {
        this.conf = conf;
        this.clientConf = clientConf;
        this.clientChannelFactory = clientChannelFactory;
        // initialize the snitch
        initialize();
    }

    protected void initialize() throws IOException {
        scheduler = new OrderedSafeExecutor(conf.getNumTopicQueuerThreads());
        // topic manager
        tm = instantiateTopicManager(scheduler);
        // persistence manager.
        pm = instantiatePersistenceManager(tm, scheduler);
        // delivery manager.
        dm = instantiateDeliveryManager(pm);
        if (dm instanceof SubChannelDisconnectedListener) {
            listener = (SubChannelDisconnectedListener) dm;
        }
        // subscription manager
        sm = instantiateSubscriptionManager(tm, pm, dm, scheduler);
        // region manager
        rm = instantiateRegionManager(pm, scheduler);
    }

    @Override
    public void start() {
        // start topic manager
        startTopicManager();
        // start persistence manager
        startPersistenceManager();
        // start delivery manager
        startDeliveryManager();
        // start subscription manager
        startSubscriptionManager();
        // start region manager
        startRegionManager();
    }

    @Override
    public void stop() {
        // stop topic manager
        stopTopicManager();
        // stop region manager
        stopRegionManager();
        // stop delivery manager
        stopDeliveryManager();
        // stop persistence manager
        stopPersistenceManager();
        // stop subscription manager
        stopSubscriptionManager();
        // shutdown the scheduler
        scheduler.shutdown();
    }

    protected void startTopicManager() {
        // do nothing
    }

    protected void stopTopicManager() {
        tm.stop();
    }

    protected abstract TopicManager instantiateTopicManager(OrderedSafeExecutor scheduler) throws IOException;

    @Override
    public TopicManager getTopicManager() {
        return tm;
    }

    protected void startPersistenceManager() {
        // do nothing now.
    }

    protected void stopPersistenceManager() {
        pm.stop();
    }

    protected abstract PersistenceManager instantiatePersistenceManager(TopicManager topicMgr,
            OrderedSafeExecutor scheduler) throws IOException;

    @Override
    public PersistenceManager getPersistenceManager() {
        return pm;
    }

    protected void startSubscriptionManager() {
        // do nothing now.
    }

    protected void stopSubscriptionManager() {
        sm.stop();
    }

    protected abstract SubscriptionManager instantiateSubscriptionManager(TopicManager tm, PersistenceManager pm,
            DeliveryManager dm, OrderedSafeExecutor scheduler) throws IOException;

    @Override
    public SubscriptionManager getSubscriptionManager() {
        return sm;
    }

    protected void startDeliveryManager() {
        dm.start();
    }

    protected void stopDeliveryManager() {
        dm.stop();
    }

    protected DeliveryManager instantiateDeliveryManager(PersistenceManager pm) {
        return new FIFODeliveryManager(pm, conf);
    }

    @Override
    public DeliveryManager getDeliveryManager() {
        return dm;
    }

    protected void startRegionManager() {
        sm.addListener(rm);
    }

    protected void stopRegionManager() {
        rm.stop();
    }

    protected RegionManager instantiateRegionManager(PersistenceManager pm, OrderedSafeExecutor scheduler) {
        return new RegionManager(pm, conf, scheduler,
                new HedwigHubClientFactory(conf, clientConf, clientChannelFactory));
    }

    @Override
    public SubChannelDisconnectedListener getSubChannelDisconnectedListener() {
        return listener;
    }

    @Override
    public void registerJMX(HedwigMBeanInfo parent) {
        if (null != pm && pm instanceof ReadAheadCache) {
            ((ReadAheadCache) pm).registerJMX(parent);
        }
    }

    @Override
    public void unregisterJMX() {
        if (null != pm && pm instanceof ReadAheadCache) {
            ((ReadAheadCache) pm).unregisterJMX();
        }
    }
}
