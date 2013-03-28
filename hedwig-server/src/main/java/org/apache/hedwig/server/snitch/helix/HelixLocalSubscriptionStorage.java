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
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.ISubscriptionTxn;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.meta.SubscriptionDataManager;
import org.apache.hedwig.util.Callback;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class HelixLocalSubscriptionStorage implements SubscriptionDataManager {

    static final Logger logger = LoggerFactory.getLogger(HelixLocalSubscriptionStorage.class);

    private static final byte SEP = '\001';

    protected final OrderedSafeExecutor ioPool;
    protected final DB subscriptionDB;
    protected final ReadOptions ro;
    protected final WriteOptions wo;
    protected final Options options;
    volatile boolean closed = false;

    protected final CacheLoader<byte[], ISubscriptionTxn> subscriptionCacheLoader = new CacheLoader<byte[], ISubscriptionTxn>() {

        @Override
        public ISubscriptionTxn load(byte[] subscriptionKey) throws Exception {
            byte[] data = subscriptionDB.get(subscriptionKey);
            if (null == data) {
                return ISubscriptionTxn.newBuilder().setTopic(ByteString.EMPTY).setSubscriberId(ByteString.EMPTY)
                        .setOp(ISubscriptionTxn.SubscriptionOp.UPDATE)
                        .setSubscriptionData(SubscriptionData.getDefaultInstance()).setVersion(-1).build();
            }
            return ISubscriptionTxn.parseFrom(data);
        }

    };
    protected final LoadingCache<byte[], ISubscriptionTxn> subscriptionCache;

    public HelixLocalSubscriptionStorage(ServerConfiguration cfg, OrderedSafeExecutor ioPool)
            throws IOException {
        String dbPath = cfg.getLeveldbSubscriptionDBPath();
        File dbDir = new File(dbPath);

        options = new Options();
        options.blockSize(cfg.getLeveldbBlockSize());
        options.cacheSize(cfg.getLeveldbSubscriptionDBCacheSize());
        options.compressionType(CompressionType.SNAPPY);
        options.createIfMissing(true);
        options.writeBufferSize(cfg.getLeveldbSubscriptionDBWriteBufferSize());
        options.maxOpenFiles(cfg.getLeveldbMaxOpenFiles());

        try {
            subscriptionDB = factory.open(dbDir, options);
        } catch (DBException dbe) {
            throw new IOException("Failed to open subscription db at " + dbPath, dbe);
        }

        // initialize read/write operations
        ro = new ReadOptions();
        ro.fillCache(true);
        wo = new WriteOptions();

        this.ioPool = ioPool;

        // initialize the subscription cache
        this.subscriptionCache = CacheBuilder.newBuilder().concurrencyLevel(cfg.getLeveldbNumIOWorkers())
                .maximumSize(cfg.getLeveldbMaxCachedSubscriptions()).build(subscriptionCacheLoader);
        logger.info("Subscription storage is served at directory {}.", dbPath);
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // close the subscription db.
        subscriptionDB.close();
    }

    private byte[] getSubscriptionKey(ByteString topic, ByteString subscriberId) {
        int size = topic.size() + subscriberId.size() + 1;
        byte[] key = new byte[size];
        topic.copyTo(key, 0, 0, topic.size());
        key[topic.size()] = SEP;
        subscriberId.copyTo(key, 0, topic.size() + 1, subscriberId.size());
        return key;
    }

    private boolean isSameTopic(byte[] topic, byte[] key) {
        return 0 == compareBytes(topic, 0, topic.length, key, 0, topic.length);
    }

    private ByteString parseSubscriberId(byte[] key, byte[] topic) {
        return ByteString.copyFrom(key, topic.length + 1, key.length - topic.length - 1);
    }

    private class ReadSubscriptionsOp extends SafeRunnable {

        final ByteString topic;
        final Callback<Map<ByteString, Versioned<SubscriptionData>>> callback;
        final Object ctx;

        ReadSubscriptionsOp(ByteString topic, Callback<Map<ByteString, Versioned<SubscriptionData>>> cb, Object ctx) {
            this.topic = topic;
            this.callback = cb;
            this.ctx = ctx;
        }

        @Override
        public void safeRun() {
            try {
                readSubscriptions();
            } catch (DBException dbe) {
                logger.error("Failed to read subscriptions for topic " + topic.toStringUtf8() + " : ", dbe);
                callback.operationFailed(
                        ctx,
                        PubSubException.create(StatusCode.SERVICE_DOWN, "Failed to read subscriptions for topic "
                                + topic.toStringUtf8() + " : " + dbe.getMessage()));
                return;
            } catch (InvalidProtocolBufferException ipbe) {
                logger.error("Failed to read subscriptions for topic " + topic.toStringUtf8() + " : ", ipbe);
                callback.operationFailed(
                        ctx,
                        PubSubException.create(StatusCode.SERVICE_DOWN, "Failed to read subscriptions for topic "
                                + topic.toStringUtf8() + " : " + ipbe.getMessage()));
                return;
            }
        }

        private void readSubscriptions() throws DBException, InvalidProtocolBufferException {
            DBIterator iter = null;
            try {
                Map<ByteString, Versioned<SubscriptionData>> states = new ConcurrentHashMap<ByteString, Versioned<SubscriptionData>>();
                iter = subscriptionDB.iterator();
                byte[] topicKey = topic.toByteArray();
                iter.seek(topicKey);
                while (iter.hasNext()) {
                    Entry<byte[], byte[]> entry = iter.next();
                    byte[] key = entry.getKey();
                    byte[] data = entry.getValue();
                    if (!isSameTopic(topicKey, key)) {
                        break;
                    }
                    ByteString subscriberId = parseSubscriberId(key, topicKey);
                    ISubscriptionTxn subMsg = ISubscriptionTxn.parseFrom(data);
                    Versioned<SubscriptionData> subData = new Versioned<SubscriptionData>(subMsg.getSubscriptionData(),
                            new ZkVersion(subMsg.getVersion()));
                    states.put(subscriberId, subData);
                }
                callback.operationFinished(ctx, states);
            } finally {
                if (null != iter) {
                    try {
                        iter.close();
                    } catch (IOException ioe) {
                    }
                }
            }
        }

    }

    @Override
    public void readSubscriptions(ByteString topic, Callback<Map<ByteString, Versioned<SubscriptionData>>> cb,
            Object ctx) {
        ioPool.submitOrdered(topic, new ReadSubscriptionsOp(topic, cb, ctx));
    }

    @Override
    public void readSubscriptionData(final ByteString topic, final ByteString subscriberId,
            final Callback<Versioned<SubscriptionData>> cb, final Object ctx) {
        ioPool.submitOrdered(topic, new SafeRunnable() {

            @Override
            public void safeRun() {
                try {
                    readSubscriptionData();
                } catch (ExecutionException e) {
                    logger.error("Failed to read subscription data for topic: " + topic.toStringUtf8()
                            + ", subscriber: " + subscriberId.toStringUtf8() + " : ", e);
                    cb.operationFailed(ctx, PubSubException.create(StatusCode.SERVICE_DOWN,
                            "Failed to read subscription data for topic: " + topic.toStringUtf8() + ", subscriber: "
                                    + subscriberId.toStringUtf8() + " : " + e.getMessage()));
                }
            }

            void readSubscriptionData() throws ExecutionException {
                ISubscriptionTxn subMsg = subscriptionCache.get(getSubscriptionKey(topic, subscriberId));
                if (subMsg.getVersion() < 0) {
                    cb.operationFinished(ctx, null);
                    return;
                }
                ZkVersion version = new ZkVersion(subMsg.getVersion());
                cb.operationFinished(ctx, new Versioned<SubscriptionData>(subMsg.getSubscriptionData(), version));
                return;
            }

        });
    }

    @Override
    public void createSubscriptionData(final ByteString topic, final ByteString subscriberId, SubscriptionData data,
            final Callback<Version> callback, final Object ctx) {
        int intVersion = -1;
        ioPool.submitOrdered(topic, new SubscriptionDataUpdater(topic, subscriberId, data, intVersion, callback, ctx,
                true));
    }

    private class SubscriptionDataUpdater extends SafeRunnable {
        final ByteString topic;
        final ByteString subscriberId;
        final SubscriptionData subData;
        final int version;
        final Callback<Version> callback;
        final Object ctx;
        final boolean isCreationOp;

        SubscriptionDataUpdater(ByteString topic, ByteString subscriberId, SubscriptionData subData, int version,
                Callback<Version> callback, Object ctx, boolean isCreationOp) {
            this.topic = topic;
            this.subscriberId = subscriberId;
            this.subData = subData;
            this.version = version;
            this.callback = callback;
            this.ctx = ctx;
            this.isCreationOp = isCreationOp;
        }

        @Override
        public void safeRun() {
            if (logger.isDebugEnabled()) {
                logger.debug("Update subscription data locally for (topic: {}, subscriber: {}) @ version {} locally.",
                        va(topic.toStringUtf8(), subscriberId.toStringUtf8(), version));
            }
            try {
                updateSubscriptionData();
            } catch (DBException dbe) {
                logger.error("Failed to create subscription data for topic " + topic.toStringUtf8() + ", subscriber "
                        + subscriberId.toStringUtf8() + " : ", dbe);
                callback.operationFailed(
                        ctx,
                        PubSubException.create(StatusCode.SERVICE_DOWN,
                                "Failed to create subscription data for topic " + topic.toStringUtf8()
                                        + ", subscriber: " + subscriberId.toStringUtf8() + " : " + dbe.getMessage()));
            } catch (ExecutionException ee) {
                logger.error("Failed to create subscription data for topic " + topic.toStringUtf8() + ", subscriber "
                        + subscriberId.toStringUtf8() + " : ", ee);
                callback.operationFailed(
                        ctx,
                        PubSubException.create(StatusCode.SERVICE_DOWN,
                                "Failed to create subscription data for topic " + topic.toStringUtf8()
                                        + ", subscriber: " + subscriberId.toStringUtf8() + " : " + ee.getMessage()));
            } catch (Throwable t) {
                logger.error(
                        "Caught unexpected exception when creating subscription data for topic " + topic.toStringUtf8()
                                + ", subscriber " + subscriberId.toStringUtf8() + " : ", t);
                callback.operationFailed(
                        ctx,
                        PubSubException.create(StatusCode.SERVICE_DOWN,
                                "Failed to create subscription data for topic " + topic.toStringUtf8()
                                        + ", subscriber: " + subscriberId.toStringUtf8() + " : " + t.getMessage()));
            }
        }

        private void updateSubscriptionData() throws DBException, ExecutionException {
            byte[] subscriptionKey = getSubscriptionKey(topic, subscriberId);
            ISubscriptionTxn subMsg = subscriptionCache.get(subscriptionKey);
            if (isCreationOp && subMsg.getVersion() >= 0) {
                callback.operationFailed(
                        ctx,
                        PubSubException.create(StatusCode.SUBSCRIPTION_STATE_EXISTS, "Subscription state for (topic:"
                                + topic.toStringUtf8() + ", subscriber:" + subscriberId.toStringUtf8() + ") existed."));
                return;
            } else if (!isCreationOp && subMsg.getVersion() < 0) {
                callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_SUBSCRIPTION_STATE,
                        "No subscription state found for (topic:" + topic.toStringUtf8() + ", subscriber:"
                                + subscriberId.toStringUtf8() + ")."));
                return;
            }
            int newVersion;
            if (version != -1) {
                if (version != subMsg.getVersion()) {
                    callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                            "Bad version provided to update subscription data for topic " + topic.toStringUtf8()
                                    + " subscriber " + subscriberId.toStringUtf8()));
                    return;
                }
                newVersion = version + 1;
            } else {
                newVersion = subMsg.getVersion() + 1;
            }
            subMsg = ISubscriptionTxn.newBuilder(subMsg).setTopic(topic).setSubscriberId(subscriberId)
                    .setVersion(newVersion).setSubscriptionData(subData).setOp(ISubscriptionTxn.SubscriptionOp.UPDATE)
                    .build();
            subscriptionDB.put(subscriptionKey, subMsg.toByteArray());
            subscriptionCache.put(subscriptionKey, subMsg);
            // use ZkVersion object for an integer version
            callback.operationFinished(ctx, new ZkVersion(newVersion));
            if (logger.isDebugEnabled()) {
                logger.debug("Finished updated subscription data locally for (topic: {}, subscriber: {}) @ new version {}.",
                        va(topic.toStringUtf8(), subscriberId.toStringUtf8(), newVersion));
            }
        }
    }

    @Override
    public boolean isPartialUpdateSupported() {
        return false;
    }

    @Override
    public void updateSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
            Version version, Callback<Version> callback, Object ctx) {
        throw new UnsupportedOperationException("Leveldb based subscription manager doesn't support partial update!");
    }

    @Override
    public void replaceSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
            Version version, Callback<Version> callback, Object ctx) {
        int intVersion = -1;
        if (Version.NEW == version) {
            callback.operationFailed(ctx, new PubSubException.BadVersionException(
                    "Can not replace Version.New subscription data"));
            return;
        } else if (Version.ANY != version) {
            if (!(version instanceof ZkVersion)) {
                callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(
                        "Invalid version provided to replace subscription data for topic  " + topic.toStringUtf8()
                                + " subscribe id: " + subscriberId));
                return;
            } else {
                intVersion = ((ZkVersion) version).getZnodeVersion();
            }
        }
        ioPool.submitOrdered(topic, new SubscriptionDataUpdater(topic, subscriberId, data, intVersion, callback, ctx,
                false));
    }

    @Override
    public void deleteSubscriptionData(final ByteString topic, final ByteString subscriberId, final Version version,
            final Callback<Void> callback, final Object ctx) {
        ioPool.submitOrdered(topic, new SafeRunnable() {

            @Override
            public void safeRun() {
                int intVersion = -1;
                if (Version.NEW == version) {
                    callback.operationFailed(ctx, new PubSubException.BadVersionException(
                            "Can not delete Version.New subscription data"));
                    return;
                } else if (Version.ANY != version) {
                    if (!(version instanceof ZkVersion)) {
                        callback.operationFailed(
                                ctx,
                                new PubSubException.UnexpectedConditionException(
                                        "Invalid version provided to delete subscription data for topic  "
                                                + topic.toStringUtf8() + " subscribe id: " + subscriberId));
                        return;
                    } else {
                        intVersion = ((ZkVersion) version).getZnodeVersion();
                    }
                }
                try {
                    deleteSubscriptionData(intVersion);
                } catch (ExecutionException e) {
                    logger.error("Failed to delete subscription for (topic:" + topic.toStringUtf8() + ", subscriber:"
                            + subscriberId.toStringUtf8() + ") : ", e);
                    callback.operationFailed(
                            ctx,
                            PubSubException.create(StatusCode.SERVICE_DOWN,
                                    "Failed to delete subscription for (topic:" + topic.toStringUtf8()
                                            + ", subscriber:" + subscriberId.toStringUtf8() + ") : " + e.getMessage()));
                } catch (DBException dbe) {
                    logger.error("Failed to delete subscription for (topic:" + topic.toStringUtf8() + ", subscriber:"
                            + subscriberId.toStringUtf8() + ") : ", dbe);
                    callback.operationFailed(
                            ctx,
                            PubSubException.create(StatusCode.SERVICE_DOWN, "Failed to delete subscription for (topic:"
                                    + topic.toStringUtf8() + ", subscriber:" + subscriberId.toStringUtf8() + ") : "
                                    + dbe.getMessage()));
                }
            }

            private void deleteSubscriptionData(int intVersion) throws ExecutionException, DBException {
                byte[] subscriptionKey = getSubscriptionKey(topic, subscriberId);
                ISubscriptionTxn subMsg = subscriptionCache.get(subscriptionKey);
                if (subMsg.getVersion() < 0) {
                    subscriptionCache.invalidate(subscriptionKey);
                    callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_SUBSCRIPTION_STATE,
                            "No subscription state found for (topic:" + topic.toStringUtf8() + ", subscriber:"
                                    + subscriberId.toStringUtf8() + ")."));
                    return;
                } else if (intVersion >= 0 && subMsg.getVersion() != intVersion) {
                    // bad version
                    callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                            "Bad version provided to delete subscription for (topic:" + topic.toStringUtf8()
                                    + ", subscriber:" + subscriberId.toStringUtf8() + ")."));
                    return;
                } else {
                    subscriptionDB.delete(subscriptionKey, wo);
                    subscriptionCache.invalidate(subscriptionKey);
                    // finished deleted the subscription entry
                    callback.operationFinished(ctx, null);
                }
            }

        });
    }

    private static int compareBytes(byte[] buf1, int off1, int len1, byte[] buf2, int off2, int len2) {
        if (buf1 == buf2 && off1 == off2 && len1 == len2) {
            return 0;
        }
        int end1 = off1 + len1;
        int end2 = off2 + len2;
        for (int i = off1, j = off2; i < end1 && j < end2; ++i, ++j) {
            int a = (buf1[i] & 0xFF);
            int b = (buf2[j] & 0xFF);
            if (a != b) {
                return a - b;
            }
        }
        return len1 - len2;
    }
}
