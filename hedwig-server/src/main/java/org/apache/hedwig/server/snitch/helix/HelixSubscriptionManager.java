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

import static org.apache.hedwig.util.VarArgs.va;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.ISubscriptionTxn;
import org.apache.hedwig.protocol.PubSubProtocol.InternalTxn;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.subscriptions.AbstractSubscriptionManager;
import org.apache.hedwig.server.subscriptions.InMemorySubscriptionState;
import org.apache.hedwig.server.topics.helix.HelixTopicManager;
import org.apache.hedwig.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class HelixSubscriptionManager extends AbstractSubscriptionManager {

    static final Logger logger = LoggerFactory.getLogger(HelixSubscriptionManager.class);

    final HelixLocalSubscriptionStorage localSubscriptionStorage;

    public HelixSubscriptionManager(ServerConfiguration cfg, HelixTopicManager tm, PersistenceManager pm,
            DeliveryManager dm, OrderedSafeExecutor scheduler) {
        super(cfg, tm, pm, dm, scheduler);
        localSubscriptionStorage = tm.getLocalSubscriptionStorage();
    }

    @Override
    protected void readSubscriptions(ByteString topic, final Callback<Map<ByteString, InMemorySubscriptionState>> cb,
            Object ctx) {
        if (logger.isDebugEnabled()) {
            logger.debug("{} read subscriptions for topic {}.", getClass().getName(), topic.toStringUtf8());
        }
        localSubscriptionStorage.readSubscriptions(topic, new Callback<Map<ByteString, Versioned<SubscriptionData>>>() {
            @Override
            public void operationFailed(Object ctx, PubSubException e) {
                cb.operationFailed(ctx, e);
            }

            @Override
            public void operationFinished(Object ctx, Map<ByteString, Versioned<SubscriptionData>> result) {
                Map<ByteString, InMemorySubscriptionState> states = new ConcurrentHashMap<ByteString, InMemorySubscriptionState>();
                for (Entry<ByteString, Versioned<SubscriptionData>> entry : result.entrySet()) {
                    Versioned<SubscriptionData> vv = entry.getValue();
                    states.put(entry.getKey(), new InMemorySubscriptionState(vv.getValue(), vv.getVersion()));
                }
                cb.operationFinished(ctx, states);
            }
        }, ctx);
    }

    @Override
    protected void readSubscriptionData(ByteString topic, ByteString subscriberId,
            final Callback<InMemorySubscriptionState> cb, Object ctx) {
        localSubscriptionStorage.readSubscriptionData(topic, subscriberId, new Callback<Versioned<SubscriptionData>>() {
            @Override
            public void operationFailed(Object ctx, PubSubException e) {
                cb.operationFailed(ctx, e);
            }

            @Override
            public void operationFinished(Object ctx, Versioned<SubscriptionData> subData) {
                if (null != subData) {
                    cb.operationFinished(ctx, new InMemorySubscriptionState(subData.getValue(), subData.getVersion()));
                } else {
                    cb.operationFinished(ctx, new InMemorySubscriptionState(SubscriptionData.getDefaultInstance(),
                            Version.NEW));
                }
            }

        }, ctx);
    }

    @Override
    protected boolean isPartialUpdateSupported() {
        return localSubscriptionStorage.isPartialUpdateSupported();
    }

    @Override
    protected void createSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
            Callback<Version> callback, Object ctx) {
        doUpdateSubscriptionData(topic, subscriberId, data, -1, callback, ctx);
    }

    @Override
    protected void updateSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
            Version version, Callback<Version> callback, Object ctx) {
        throw new UnsupportedOperationException("Local subscription storage manager doesn't support partial update!");
    }

    @Override
    protected void replaceSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
            Version version, Callback<Version> callback, Object ctx) {
        doUpdateSubscriptionData(topic, subscriberId, data, ((ZkVersion) version).getZnodeVersion(), callback, ctx);
    }

    private void doUpdateSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
            final int version, final Callback<Version> callback, final Object context) {
        if (logger.isDebugEnabled()) {
            logger.debug("Update subscription data for (topic: {}, subscriber: {}) @ version {} : {}",
                    va(topic.toStringUtf8(), subscriberId.toStringUtf8(), version, data));
        }
        final TopicInfo topicInfo = this.top2sub2seq.get(topic);
        final Object topicContext;
        if (null == topicInfo || !((topicContext = topicInfo.getTopicContext()) instanceof TxnLog)) {
            callback.operationFailed(context, PubSubException.create(StatusCode.SERVICE_DOWN,
                    "No partition txn log found to update subscription data for topic " + topic.toStringUtf8()
                            + ", subscriber: " + subscriberId.toStringUtf8()));
            return;
        }
        TxnLog txnLog = (TxnLog) topicContext;
        ISubscriptionTxn.Builder subTxn = ISubscriptionTxn.newBuilder().setOp(ISubscriptionTxn.SubscriptionOp.UPDATE)
                .setSubscriberId(subscriberId).setTopic(topic).setSubscriptionData(data).setVersion(version);
        InternalTxn.Builder txn = InternalTxn.newBuilder().setType(InternalTxn.Type.SUBSCRIPTION).setSubTxn(subTxn);
        txnLog.processTxn(txn, new Callback<Void>() {

            @Override
            public void operationFailed(Object ctx, PubSubException e) {
                callback.operationFailed(context, e);
            }

            @Override
            public void operationFinished(Object ctx, Void e) {
                // TODO: think a bit more how to pass version back.
                callback.operationFinished(context, new ZkVersion(version + 1));
            }

        });
    }

    @Override
    protected void deleteSubscriptionData(ByteString topic, ByteString subscriberId, final Version version,
            final Callback<Void> callback, final Object context) {
        final TopicInfo topicInfo = this.top2sub2seq.get(topic);
        final Object topicContext;
        if (null == topicInfo || !((topicContext = topicInfo.getTopicContext()) instanceof TxnLog)) {
            callback.operationFailed(context, PubSubException.create(StatusCode.SERVICE_DOWN,
                    "No partition txn log found to update subscription data for topic " + topic.toStringUtf8()
                            + ", subscriber: " + subscriberId.toStringUtf8()));
            return;
        }
        TxnLog txnLog = (TxnLog) topicContext;
        ISubscriptionTxn.Builder subTxn = ISubscriptionTxn.newBuilder().setOp(ISubscriptionTxn.SubscriptionOp.UPDATE)
                .setSubscriberId(subscriberId).setTopic(topic).setVersion(((ZkVersion) version).getZnodeVersion());
        InternalTxn.Builder txn = InternalTxn.newBuilder().setType(InternalTxn.Type.SUBSCRIPTION).setSubTxn(subTxn);
        txnLog.processTxn(txn, new Callback<Void>() {

            @Override
            public void operationFailed(Object ctx, PubSubException e) {
                callback.operationFailed(context, e);
            }

            @Override
            public void operationFinished(Object ctx, Void e) {
                // TODO: think a bit more how to pass version back.
                callback.operationFinished(context, e);
            }

        });

    }

    @Override
    public void stop() {
        super.stop();
        try {
            localSubscriptionStorage.close();
        } catch (IOException e) {
            logger.error("Failed to close local subscription storage for Helix snitch : ", e);
        }
        logger.info("Stop local subscription store.");
    }

}
