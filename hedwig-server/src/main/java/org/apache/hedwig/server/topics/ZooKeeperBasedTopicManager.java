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
package org.apache.hedwig.server.topics;

import java.net.UnknownHostException;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooKeeper;

import com.google.protobuf.ByteString;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.TopicOpQueuer;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.util.Either;
import org.apache.hedwig.util.HedwigSocketAddress;

/**
 * Topic Managers leverage ZooKeeper as hub server manager.
 */
public abstract class ZooKeeperBasedTopicManager extends AbstractTopicManager {

    static Logger logger = LoggerFactory.getLogger(ZooKeeperBasedTopicManager.class);

    // ZooKeeper Handle
    protected ZooKeeper zk;

    // hub server manager
    protected final HubServerManager hubManager;

    protected volatile HubInfo myHubInfo;
    protected final HubLoad myHubLoad;

    // Boolean flag indicating if we should suspend activity. If this is true,
    // all of the Ops put into the queuer will fail automatically.
    protected volatile boolean isSuspended = false;

    // An service handling state changes in hub server
    protected final ScheduledExecutorService stateService =
        Executors.newSingleThreadScheduledExecutor();

    // Runnable to execute reclaim task after recovering from expire
    protected Runnable reclaimRunnable = new Runnable() {
        @Override
        public void run() {
            if (isSuspended) {
                // the manager is suspended again, so we don't need to try to register itself again
                return;
            }
            try {
                HubInfo newHubInfo = registerSelf();
                if (null != myHubInfo && myHubInfo.equals(newHubInfo)) {
                    logger.info("Give up reclaiming topic ownerships since hub server is still in same session.");
                    return;
                }
                myHubInfo = newHubInfo;
            } catch (PubSubException pse) {
                logger.error("Failed to register itself recovering from session expire : ", pse);
                if (!isSuspended) {
                    // if the manager is suspended again, we don't need to retry
                    try {
                        stateService.schedule(this, cfg.getZkTimeout(), TimeUnit.MILLISECONDS); 
                    } catch (RejectedExecutionException ree) {
                        logger.warn("Failed to schedule resume operation after recovered from session expire : ", ree);
                    }
                }
                return;
            }
            // reclaim its owned topics
            for (ByteString topic : topics) {
                reclaimOwnershipAfterExpired(topic);
            }
        }
    };

    /**
     * Create a new topic manager.
     *
     * @param zk
     *          ZooKeeper Handle.
     * @param cfg
     *          Server Configuration Object.
     * @param scheduler
     *          Scheduler used for topic manager operations.
     */
    ZooKeeperBasedTopicManager(final ZooKeeper zk, final ServerConfiguration cfg,
                               ScheduledExecutorService scheduler)
            throws UnknownHostException, PubSubException {

        super(cfg, scheduler);
        this.zk = zk;
        this.hubManager = new ZkHubServerManager(cfg, zk, addr);

        myHubLoad = new HubLoad(topics.size());
        this.hubManager.registerListener(new HubServerManager.ManagerListener() {
            @Override
            public void onSuspend() {
                isSuspended = true;
            }
            @Override
            public void onResume() {
                // it was OK to mark the state to be normal immediately
                // the topic ownership is also guaranteed by ledger fencing
                // so no publish would succeed if the topic ownership is changed before reclaiming,
                // and the ownership would be released automatically if topic ownership changed.
                isSuspended = false;
                try {
                    stateService.submit(reclaimRunnable);
                } catch (RejectedExecutionException ree) {
                    logger.warn("Failed to execute resume operation after recovered from session expire : ", ree);
                }
            }
            @Override
            public void onShutdown() {
                // if hub server manager can't work, we had to quit
                Runtime.getRuntime().exit(1);
            }
        });

        myHubInfo = registerSelf();
        logger.info("Start metadata manager based topic manager with hub id : " + myHubInfo);
    }

    private HubInfo registerSelf() throws PubSubException {
        myHubLoad.setNumTopics(topics.size());
        final SynchronousQueue<Either<HubInfo, PubSubException>> queue =
            new SynchronousQueue<Either<HubInfo, PubSubException>>();
        this.hubManager.registerSelf(myHubLoad, new Callback<HubInfo>() {
            @Override
            public void operationFinished(final Object ctx, final HubInfo resultOfOperation) {
                logger.info("Successfully registered hub {} with zookeeper", resultOfOperation);
                ConcurrencyUtils.put(queue, Either.of(resultOfOperation, (PubSubException) null));
            }
            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                logger.error("Failed to register hub with zookeeper", exception);
                ConcurrencyUtils.put(queue, Either.of((HubInfo)null, exception));
            }
        }, null);

        Either<HubInfo, PubSubException> result = ConcurrencyUtils.take(queue);
        PubSubException pse = result.right();
        if (pse != null) {
            throw pse;
        }
        return result.left();
    }

    @Override
    public HubServerManager getHubServerManager() {
        return hubManager;
    }

    void reclaimOwnershipAfterExpired(final ByteString topic) {
        queuer.pushAndMaybeRun(topic, new ReclaimOwnerOp(topic, new Callback<HedwigSocketAddress>() {
            @Override
            public void operationFinished(Object ctx, HedwigSocketAddress owner) {
                if (!owner.equals(addr)) {
                    // the topic has been owned by other topics.
                    // release the ownership.
                    releaseTopic(topic, RELEASE_CALLBACK, topic);
                    return;
                }
                topics.add(topic);
            }
            @Override
            public void operationFailed(final Object ctx, final PubSubException exception) {
                // the manager is suspened again, we don't need to retry reclaiming
                // since when the manager is resumed again, it would start relcaiming.
                if (!isSuspended) {
                    reclaimOwnershipAfterExpired(topic);
                }
            }
        }, null));
    }

    private class ReclaimOwnerOp extends TopicOpQueuer.AsynchronousOp<HedwigSocketAddress> {

        ReclaimOwnerOp(ByteString topic, Callback<HedwigSocketAddress> cb, Object ctx) {
            queuer.super(topic, cb, ctx);
        }

        @Override
        public void run() {
            if (isSuspended) {
                return;
            }
            reclaimOwnership(topic, cb, ctx); 
        }
    }

    /**
     * Reclaim ownership for topic.
     *
     * @param topic
     *          Topic Name.
     * @param cb
     *          Callback after topic is reclaimed.
     * @param ctx
     *          Callback context.
     */
    protected abstract void reclaimOwnership(ByteString topic, Callback<HedwigSocketAddress> cb, Object ctx);

    @Override
    protected void realGetOwner(final ByteString topic, final boolean shouldClaim,
                                final Callback<HedwigSocketAddress> cb, final Object ctx) {
        if (topics.contains(topic)) {
            cb.operationFinished(ctx, addr);
            return;
        }

        // If the topic manager is suspended, we could not get owner or claim anything
        // so error out this call and return.
        if (isSuspended) {
            // respond not responsible when topic manager is suspend and the hub server
            // doesn't own the topic.
            cb.operationFailed(ctx, new PubSubException.ServerNotResponsibleForTopicException(""));
            return;
        }

        doGetOwner(topic, shouldClaim, cb, ctx);
    }

    protected abstract void doGetOwner(
            final ByteString topic, final boolean shouldClaim,
            final Callback<HedwigSocketAddress> cb, final Object ctx);

    @Override
    public void stop() {
        // shutdown state service
        try {
            stateService.shutdown();
        } catch (Exception e) {
            logger.error("Error shutting down state service : ", e);
        }
        // we just unregister it with zookeeper to make it unavailable from hub servers list
        try {
            hubManager.unregisterSelf();
        } catch (IOException e) {
            logger.error("Error unregistering hub server :", e);
        }
        super.stop();
    }

}
