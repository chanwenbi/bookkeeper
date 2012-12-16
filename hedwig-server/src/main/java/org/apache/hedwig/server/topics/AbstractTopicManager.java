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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.TopicOpQueuer;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.CallbackUtils;
import org.apache.hedwig.util.HedwigSocketAddress;

public abstract class AbstractTopicManager implements TopicManager {
    /**
     * My name.
     */
    protected HedwigSocketAddress addr;

    /**
     * Topic change listeners.
     */
    protected ArrayList<TopicOwnershipChangeListener> listeners = new ArrayList<TopicOwnershipChangeListener>();

    /**
     * List of topics I believe I am responsible for.
     */
    protected Set<ByteString> topics = Collections.synchronizedSet(new HashSet<ByteString>());

    protected ChainedTopicOpQueuer queuer;
    protected ServerConfiguration cfg;
    protected ScheduledExecutorService retentionScheduler;

    private static final Logger logger = LoggerFactory.getLogger(AbstractTopicManager.class);

    private class ChainedTopicOpQueuer extends TopicOpQueuer {
        public ChainedTopicOpQueuer(OrderedSafeExecutor scheduler) {
            super(scheduler);
        }

        public void popAndRunNextsWithException(ByteString topic, PubSubException exception) {
            Queue<Runnable> ops;
            synchronized (this) {
                ops = topic2ops.get(topic);
                if (null == ops) {
                    return;
                }
            }
            synchronized (ops) {
                if (!ops.isEmpty()) {
                    Runnable op = ops.remove();
                    // if get owner op is failed
                    if (op instanceof GetOwnerOp) {
                        // failed the following get owner ops
                        while (!ops.isEmpty()) {
                            Runnable r = ops.peek();    
                            if (r instanceof GetOwnerOp) {
                                ((GetOwnerOp)r).failOriginalCallback(exception);
                                ops.remove();
                            } else if (r instanceof ReleaseOp) {
                                scheduler.unsafeSubmitOrdered(topic, r);
                                break;
                            }
                        }
                    } else {
                        // for release op, it failed which means the ownership doesn't release
                        // we have to execute following request.
                        if (!ops.isEmpty()) {
                            scheduler.unsafeSubmitOrdered(topic, ops.peek());
                        }
                    }
                }
            }
        }

        public void popAndRunNextsWithResult(ByteString topic, Object result) {
            Queue<Runnable> ops;
            synchronized (this) {
                ops = topic2ops.get(topic);
                if (null == ops) {
                    return;
                }
            }
            synchronized (ops) {
                if (!ops.isEmpty()) {
                    Runnable op = ops.remove();
                    // if get owner op is failed
                    if (op instanceof GetOwnerOp) {
                        // failed the following get owner ops
                        while (!ops.isEmpty()) {
                            Runnable r = ops.peek();    
                            if (r instanceof GetOwnerOp) {
                                HedwigSocketAddress owner = (HedwigSocketAddress)result;
                                // if topic ownership is not itself, redirects all pending requests
                                if (!owner.equals(addr)) {
                                    ((GetOwnerOp)r).succeedOriginalCallback(owner);
                                    ops.remove();
                                } else {
                                    scheduler.unsafeSubmitOrdered(topic, r);
                                    break;
                                }
                            } else if (r instanceof ReleaseOp) {
                                scheduler.unsafeSubmitOrdered(topic, r);
                                break;
                            }
                        }
                    } else {
                        // for release op, it succeed which means the ownership isn't itself
                        // we have to execute following request.
                        if (!ops.isEmpty()) {
                            scheduler.unsafeSubmitOrdered(topic, ops.peek());
                        }
                    }
                }
            }
        }

        public abstract class TopicOp<T> extends TopicOpQueuer.AsynchronousOp<T> {

            public Callback<T> originalCb;

            public TopicOp(final ByteString topic, final Callback<T> cb, Object ctx) {
                super(topic, cb, ctx);
                this.originalCb = cb;
                // overwrite the default callback
                this.cb = new Callback<T>() {
                    @Override
                    public void operationFailed(Object ctx, PubSubException exception) {
                        originalCb.operationFailed(ctx, exception);
                        popAndRunNextsWithException(topic, exception);
                    }
                    @Override
                    public void operationFinished(Object ctx, T resultOfOperation) {
                        originalCb.operationFinished(ctx, resultOfOperation);
                        popAndRunNextsWithResult(topic, resultOfOperation);
                    }
                };
            }

            public void failOriginalCallback(PubSubException ex) {
                this.originalCb.operationFailed(this.ctx, ex);
            }

            public void succeedOriginalCallback(T resultOfOperation) {
                this.originalCb.operationFinished(this.ctx, resultOfOperation);
            }
        }
    }

    private class GetOwnerOp extends ChainedTopicOpQueuer.TopicOp<HedwigSocketAddress> {
        public boolean shouldClaim;

        public GetOwnerOp(final ByteString topic, boolean shouldClaim,
                          final Callback<HedwigSocketAddress> cb, Object ctx) {
            queuer.super(topic, cb, ctx);
            this.shouldClaim = shouldClaim;
        }

        @Override
        public void run() {
            realGetOwner(topic, shouldClaim, cb, ctx);
        }
    }

    private class ReleaseOp extends ChainedTopicOpQueuer.TopicOp<Void> {
        public ReleaseOp(ByteString topic, Callback<Void> cb, Object ctx) {
            queuer.super(topic, cb, ctx);
        }

        @Override
        public void run() {
            if (!topics.contains(topic)) {
                cb.operationFinished(ctx, null);
                return;
            }
            realReleaseTopic(topic, cb, ctx);
        }
    }

    public AbstractTopicManager(ServerConfiguration cfg, OrderedSafeExecutor scheduler)
            throws UnknownHostException {
        this.cfg = cfg;
        this.queuer = new ChainedTopicOpQueuer(scheduler);
        // retention scheduler only used for topic retention, so for simple
        // use a scheduled thread pool directly. we don't need to partition topics
        // since it would just run an asynchronous op (releaseTopic) to enqueue to the
        // real worker
        this.retentionScheduler =
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        addr = cfg.getServerAddr();
    }

    @Override
    public synchronized void addTopicOwnershipChangeListener(TopicOwnershipChangeListener listener) {
        listeners.add(listener);
    }

    protected final synchronized void notifyListenersAndAddToOwnedTopics(final ByteString topic,
            final Callback<HedwigSocketAddress> originalCallback, final Object originalContext) {

        Callback<Void> postCb = new Callback<Void>() {

            @Override
            public void operationFinished(Object ctx, Void resultOfOperation) {
                topics.add(topic);
                if (cfg.getRetentionSecs() > 0) {
                    retentionScheduler.schedule(new Runnable() {
                        @Override
                        public void run() {
                            // Enqueue a release operation. (Recall that release
                            // doesn't "fail" even if the topic is missing.)
                            releaseTopic(topic, new Callback<Void>() {

                                @Override
                                public void operationFailed(Object ctx, PubSubException exception) {
                                    logger.error("failure that should never happen when periodically releasing topic "
                                                 + topic, exception);
                                }

                                @Override
                                public void operationFinished(Object ctx, Void resultOfOperation) {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("successful periodic release of topic "
                                            + topic.toStringUtf8());
                                    }
                                }

                            }, null);
                        }
                    }, cfg.getRetentionSecs(), TimeUnit.SECONDS);
                }
                originalCallback.operationFinished(originalContext, addr);
            }

            @Override
            public void operationFailed(final Object ctx, final PubSubException exception) {
                // TODO: optimization: we can release this as soon as we experience the first error.
                Callback<Void> cb = new Callback<Void>() {
                    public void operationFinished(Object _ctx, Void _resultOfOperation) {
                        originalCallback.operationFailed(ctx, exception);
                    }
                    public void operationFailed(Object _ctx, PubSubException _exception) {
                        logger.error("Exception releasing topic", _exception);
                        originalCallback.operationFailed(ctx, exception);
                    }
                };
                
                realReleaseTopic(topic, cb, originalContext);
            }
        };

        Callback<Void> mcb = CallbackUtils.multiCallback(listeners.size(), postCb, null);
        for (TopicOwnershipChangeListener listener : listeners) {
            listener.acquiredTopic(topic, mcb, null);
        }
    }

    private void realReleaseTopic(ByteString topic, Callback<Void> callback, Object ctx) {
        for (TopicOwnershipChangeListener listener : listeners)
            listener.lostTopic(topic);
        topics.remove(topic);
        postReleaseCleanup(topic, callback, ctx);
    }

    @Override
    public final void getOwner(ByteString topic, boolean shouldClaim,
                               Callback<HedwigSocketAddress> cb, Object ctx) {
        queuer.pushAndMaybeRun(topic, new GetOwnerOp(topic, shouldClaim, cb, ctx));
    }

    @Override
    public final void releaseTopic(ByteString topic, Callback<Void> cb, Object ctx) {
        queuer.pushAndMaybeRun(topic, new ReleaseOp(topic, cb, ctx));
    }

    /**
     * This method should "return" the owner of the topic if one has been chosen
     * already. If there is no pre-chosen owner, either this hub or some other
     * should be chosen based on the shouldClaim parameter. If its ends up
     * choosing this hub as the owner, the {@code
     * AbstractTopicManager#notifyListenersAndAddToOwnedTopics(ByteString,
     * OperationCallback, Object)} method must be called.
     *
     */
    protected abstract void realGetOwner(ByteString topic, boolean shouldClaim,
                                         Callback<HedwigSocketAddress> cb, Object ctx);

    /**
     * The method should do any cleanup necessary to indicate to other hubs that
     * this topic has been released
     */
    protected abstract void postReleaseCleanup(ByteString topic, Callback<Void> cb, Object ctx);

    @Override
    public void stop() {
        // do nothing now
    }
}
