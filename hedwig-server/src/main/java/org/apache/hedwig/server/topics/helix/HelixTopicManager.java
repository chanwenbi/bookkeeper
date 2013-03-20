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

import static org.apache.hedwig.util.VarArgs.va;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.versioning.Version;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.IDataTxn;
import org.apache.hedwig.protocol.PubSubProtocol.IFenceTxn;
import org.apache.hedwig.protocol.PubSubProtocol.ISubscriptionTxn;
import org.apache.hedwig.protocol.PubSubProtocol.InternalTxn;
import org.apache.hedwig.protocol.PubSubProtocol.InternalTxn.Builder;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PartitionData;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.TopicOpQueuer;
import org.apache.hedwig.server.common.TopicOpQueuer.AsynchronousOp;
import org.apache.hedwig.server.common.TopicOpQueuer.SynchronousOp;
import org.apache.hedwig.server.regions.HedwigHubClientFactory;
import org.apache.hedwig.server.snitch.helix.HelixLocalSubscriptionStorage;
import org.apache.hedwig.server.snitch.helix.HelixPersistenceManager;
import org.apache.hedwig.server.snitch.helix.TxnLog;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TopicOwnershipChangeListener;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.CallbackUtils;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.spectator.RoutingTableProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class HelixTopicManager extends StateModelFactory<TopicStateModel> implements TopicManager,
        TopicPartitionOwnershipListener {

    static final Logger logger = LoggerFactory.getLogger(HelixTopicManager.class);

    public static final String DEFAULT_NAMESPACE = "hedwig";
    public static final String DEFAULT_CLUSTERNAME = "hedwig";

    static enum State {
        OFFLINE(0), BOOTSTRAPING(1), BOOTSTAPPED(2), FENCING(3), FENCED(4), FAILED(-1);

        int priority;

        State(int priority) {
            this.priority = priority;
        }

        boolean stricterThan(State other) {
            return priority >= other.priority;
        }
    }

    class TopicPartitionTxnLog implements TxnLog {

        class RequestKey {
            long epoch;
            long txnid;

            RequestKey(long epoch, long txnid) {
                this.epoch = epoch;
                this.txnid = txnid;
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder().append("(epoch:").append(epoch).append(", txnid:").append(txnid)
                        .append(")");
                return sb.toString();
            }

            @Override
            public boolean equals(Object o) {
                if (!(o instanceof RequestKey)) {
                    return false;
                }
                RequestKey otherKey = (RequestKey) o;
                return epoch == otherKey.epoch && txnid == otherKey.txnid;
            }

            @Override
            public int hashCode() {
                return (int) txnid;
            }
        }

        final long epoch;
        final AtomicLong txnidGen = new AtomicLong(0);
        final ByteString logName;
        final ConcurrentMap<RequestKey, Callback<Void>> pendingTxns = new ConcurrentHashMap<RequestKey, Callback<Void>>();

        TopicPartitionTxnLog(long epoch, String partitionName) {
            this.epoch = epoch;
            this.logName = partitionToHedwigTopic(partitionName);
        }

        @Override
        public void processTxn(Builder txnBuilder, Callback<Void> callback) {
            long txnid = txnidGen.incrementAndGet();
            InternalTxn txn = txnBuilder.setEpoch(epoch).setTxnId(txnid).build();
            Message msg = Message.newBuilder().setBody(txn.toByteString()).build();
            final RequestKey key = new RequestKey(epoch, txnid);
            pendingTxns.put(key, callback);
            hubClient.getPublisher().asyncPublishWithResponse(logName, msg, new Callback<PublishResponse>() {

                @Override
                public void operationFailed(Object ctx, PubSubException e) {
                    Callback<Void> cb = pendingTxns.remove(key);
                    if (null != cb) {
                        cb.operationFailed(ctx, e);
                    }
                }

                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    // do nothing, since we need let the message go to the
                    // subscriber first. the subscriber would notify the txn
                    // succeed or not.
                }

            }, null);
        }

        void onTxnSucceed(long epoch, long txnid) {
            if (logger.isDebugEnabled()) {
                logger.debug("Request (epoch: {}, txnid{}) for partition {} completed.",
                        va(epoch, txnid, logName.toStringUtf8()));
            }
            RequestKey key = new RequestKey(epoch, txnid);
            Callback<Void> cb = pendingTxns.remove(key);
            if (null != cb) {
                cb.operationFinished(null, null);
            }
        }

        void onTxnFailed(long epoch, long txnid, PubSubException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Request (epoch: {}, txnid{}) for partition {} failed : {}",
                        va(epoch, txnid, logName.toStringUtf8(), e.getMessage()));
            }
            RequestKey key = new RequestKey(epoch, txnid);
            Callback<Void> cb = pendingTxns.remove(key);
            if (null != cb) {
                cb.operationFailed(null, e);
            }
        }

        void failAllPendingTxns() {
            for (Callback<Void> cb : pendingTxns.values()) {
                cb.operationFailed(null, null);
            }
            pendingTxns.clear();
        }

    }

    /**
     * Topic Partition maintains the topics it already claimed for a partition.
     */
    class TopicPartition implements MessageHandler {

        final String partitionName;
        // State of current partition
        volatile State state = State.OFFLINE;
        // Epoch of current partition
        long epoch = -1L;
        // List of topics I believe I am responsible for.
        final Set<ByteString> topics = new HashSet<ByteString>();
        // Txn Log for this partition
        volatile TopicPartitionTxnLog txnLog = null;

        public TopicPartition(String partitionName) {
            this.partitionName = partitionName;
        }

        boolean canBoostrap() {
            return State.OFFLINE == state;
        }

        boolean canFence() {
            return State.BOOTSTAPPED == state;
        }

        boolean canDowngradeToBoostrap() {
            return state.stricterThan(State.BOOTSTAPPED);
        }

        boolean canOffline() {
            return State.BOOTSTRAPING.stricterThan(state);
        }

        TopicPartition markBoostapping() {
            state = State.BOOTSTRAPING;
            return this;
        }

        TopicPartition markBoostapped() {
            state = State.BOOTSTAPPED;
            return this;
        }

        TopicPartition markOffline() {
            state = State.OFFLINE;
            return this;
        }

        TopicPartition markFencing() {
            state = State.FENCING;
            return this;
        }

        TopicPartition markFenced() {
            state = State.FENCED;
            return this;
        }

        TopicPartition markFailed() {
            state = State.FAILED;
            return this;
        }

        boolean isLeader() {
            return State.FENCED == state;
        }

        State getState() {
            return state;
        }

        void setTxnLog(TopicPartitionTxnLog txnLog) {
            TopicPartitionTxnLog oldLog = this.txnLog;
            if (null != oldLog) {
                oldLog.failAllPendingTxns();
            }
            this.txnLog = txnLog;
        }

        private void failTxn(long epoch, long txnid, PubSubException e) {
            TopicPartitionTxnLog txnLog = this.txnLog;
            if (null != txnLog) {
                txnLog.onTxnFailed(epoch, txnid, e);
            }
        }

        private void finishTxn(long epoch, long txnid) {
            TopicPartitionTxnLog txnLog = this.txnLog;
            if (null != txnLog) {
                txnLog.onTxnSucceed(epoch, txnid);
            }
        }

        @Override
        public void deliver(ByteString topic, ByteString subscriber, Message msg, Callback<Void> cb, Object ctx) {
            // only accept message when I am in a state stricter than BOOTSTAPPED
            if (!state.stricterThan(State.BOOTSTAPPED)) {
                logger.debug("Ignore received messages since partition {} is not bootstrapped yet.", partitionName);
                return;
            }
            InternalTxn itxn;
            try {
                itxn = InternalTxn.parseFrom(msg.getBody());
            } catch (InvalidProtocolBufferException e) {
                // TODO: simply dropped the bad message now, since we have no
                // idea how to handle it.
                logger.warn("Invalid message received for partition {}.", partitionName);
                return;
            }
            if (itxn.getEpoch() < epoch) {
                // drop this message and fail the callback
                logger.warn("{} received a {} txn from old epoch {}, now is {}.",
                        va(partitionName, itxn.getType(), itxn.getEpoch(), epoch));
                failTxn(itxn.getEpoch(),
                        itxn.getTxnId(),
                        PubSubException.create(StatusCode.TOPIC_BUSY, "Received a " + itxn.getType() + " at old epoch "
                                + itxn.getEpoch()));
                cb.operationFinished(ctx, null);
                return;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("{} received a {} txn({}) from epoch {}.",
                        va(partitionName, itxn.getType(), itxn.getTxnId(), itxn.getEpoch()));
            }
            if (InternalTxn.Type.DATA == itxn.getType()) {
                processDataTxn(itxn.getEpoch(), itxn.getTxnId(), itxn.getDataTxn(), cb, ctx);
            } else if (InternalTxn.Type.SUBSCRIPTION == itxn.getType()) {
                processSubscriptionTxn(itxn.getEpoch(), itxn.getTxnId(), itxn.getSubTxn(), cb, ctx);
            } else {
                processFenceTxn(itxn.getEpoch(), itxn.getTxnId(), itxn.getFenceTxn(), cb, ctx);
            }
        }

        void processDataTxn(long iepoch, long itxnid, IDataTxn idata, Callback<Void> consumeCb, Object ctx) {
            // parse the message first
            Message realMsg;
            try {
                realMsg = Message.parseFrom(idata.getBody());
            } catch (InvalidProtocolBufferException e) {
                logger.warn("Received invalid message from topic " + idata.getTopic().toStringUtf8()
                        + ", drop it : ", e);
                failTxn(iepoch, itxnid, PubSubException.create(StatusCode.TOPIC_BUSY, "Invalid message for (epoch:" + iepoch + ", txnid:" + itxnid + ")."));
                consumeCb.operationFinished(ctx, null);
                return;
            }
            // notify the listener that a message is persisted
            finishTxn(iepoch, itxnid);
            // persist receive message
            persistMessage(idata.getTopic(), realMsg, consumeCb, ctx);
        }

        void processFenceTxn(long iepoch, long itxnid, IFenceTxn ifence, Callback<Void> consumeCb, Object ctx) {
            epoch = iepoch;
            // notify the listener that a partition is fenced.
            finishTxn(iepoch, itxnid);
            persistPartitionData(epoch, consumeCb, ctx);
        }

        void processSubscriptionTxn(long iepoch, long itxnid, ISubscriptionTxn isub, Callback<Void> consumeCb, Object ctx) {
            if (ISubscriptionTxn.SubscriptionOp.DELETE == isub.getOp()) {
                deleteSubscriptionData(iepoch, itxnid, isub, consumeCb, ctx);
            } else if (ISubscriptionTxn.SubscriptionOp.UPDATE == isub.getOp()) {
                persistSubscriptionData(iepoch, itxnid, isub, consumeCb, ctx);
            } else {
                logger.error("{} : unknown subscription txn {} received in epoch {}, txnid {}.",
                        va(partitionName, isub.getOp(), iepoch, itxnid));
                PubSubException e = PubSubException.create(StatusCode.UNEXPECTED_CONDITION, "Unknown subscription " + isub.getOp()
                                + " txn.");
                failTxn(iepoch, itxnid, e);
                consumeCb.operationFinished(ctx, null);
            }
        }

        private void persistMessage(final ByteString topic, Message msg, final Callback<Void> cb, Object ctx) {
            final MessageSeqId msgId = msg.getMsgId();
            persistenceManager.persistMessage(topic, msgId, msg, new Callback<MessageSeqId>() {
                @Override
                public void operationFailed(Object ctx, PubSubException e) {
                    cb.operationFailed(ctx, e);
                }

                @Override
                public void operationFinished(Object ctx, MessageSeqId seqId) {
                    cb.operationFinished(ctx, null);
                }
            }, ctx);
        }

        private void persistPartitionData(long epoch, Callback<Void> callback, Object ctx) {
            PartitionData data = PartitionData.newBuilder().setEpoch(epoch).build();
            persistenceManager.updatePartitionData(partitionName, data, callback, ctx);
        }

        private void readPartitionData(Callback<PartitionData> callback, Object ctx) {
            persistenceManager.readPartitionData(partitionName, callback, ctx);
        }

        private void persistSubscriptionData(final long iepoch, final long itxnid, final ISubscriptionTxn isub,
                final Callback<Void> callback, Object ctx) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "{} persist subscription data for topic {}, subscriber {}, version {} from (epoch:{}, txnid:{})",
                        va(partitionName, isub.getTopic().toStringUtf8(), isub.getSubscriberId().toStringUtf8(),
                                isub.getVersion(), iepoch, itxnid));
            }
            Callback<Version> persistCb = new Callback<Version>() {
                @Override
                public void operationFailed(Object ctx, PubSubException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "{} failed to persist subscription data for topic {}, subscriber {} @ version {} from (epoch: {}, txnid: {})",
                                va(partitionName, isub.getTopic().toStringUtf8(),
                                        isub.getSubscriberId().toStringUtf8(), isub.getVersion(), iepoch, itxnid));
                    }
                    failTxn(iepoch, itxnid, e);
                    if (e.getCode() == StatusCode.BAD_VERSION) {
                        // if this is a bad version operation, drop the message.
                        callback.operationFinished(ctx, null);
                    } else {
                        callback.operationFailed(ctx, e);
                    }
                }
                @Override
                public void operationFinished(Object ctx, Version version) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "{} finished persisting subscription data for topic {}, subscriber {} @ version {} from (epoch: {}, txnid: {})",
                                va(partitionName, isub.getTopic().toStringUtf8(),
                                        isub.getSubscriberId().toStringUtf8(), isub.getVersion(), iepoch, itxnid));
                    }
                    finishTxn(iepoch, itxnid);
                    callback.operationFinished(ctx, null);
                }
            };
            if (isub.getVersion() < 0) {
                localSubscriptionStore.createSubscriptionData(isub.getTopic(), isub.getSubscriberId(), isub.getSubscriptionData(), persistCb, ctx);
            } else {
                localSubscriptionStore.updateSubscriptionData(isub.getTopic(), isub.getSubscriberId(), isub.getSubscriptionData(), new ZkVersion(isub.getVersion()), persistCb, ctx);
            }
        }

        private void deleteSubscriptionData(final long iepoch, final long itxnid, final ISubscriptionTxn isub,
                final Callback<Void> callback, final Object ctx) {
            Callback<Void> deleteCb = new Callback<Void>() {
                @Override
                public void operationFailed(Object ctx, PubSubException e) {
                    failTxn(iepoch, itxnid, e);
                    if (e.getCode() == StatusCode.BAD_VERSION) {
                        // if this is a bad version operation, drop the message.
                        callback.operationFinished(ctx, null);
                    } else {
                        callback.operationFailed(ctx, e);
                    }
                }

                @Override
                public void operationFinished(Object ctx, Void result) {
                    finishTxn(iepoch, itxnid);
                    callback.operationFinished(ctx, null);
                }
            };
            localSubscriptionStore.deleteSubscriptionData(isub.getTopic(), isub.getSubscriberId(),
                    new ZkVersion(isub.getVersion()), deleteCb, ctx);
        }

        public void releaseTopics() {
            setTxnLog(null);
            int numTopics = topics.size();
            Callback<Void> postCb = new Callback<Void>() {
                @Override
                public void operationFinished(Object ctx, Void result) {
                    logger.info("Released all the topics for topic partition {} successfully.", partitionName);
                }

                @Override
                public void operationFailed(Object ctx, PubSubException e) {
                    logger.error("Failed to release all the topics for topic partition " + partitionName + " : ", e);
                }
            };
            Callback<Void> mcb = CallbackUtils.multiCallback(numTopics, postCb, null);
            for (ByteString topic : topics) {
                realReleaseTopic(this, topic, mcb, null);
            }
        }
    }

    abstract class AsyncPartitionOp<T> extends AsynchronousOp<T> {
        final protected String partitionName;

        public AsyncPartitionOp(String partitionName, Callback<T> cb, Object ctx) {
            queuer.super(ByteString.copyFromUtf8(partitionName), cb, ctx);
            this.partitionName = partitionName;
        }
    }

    abstract class SyncPartitionOp extends SynchronousOp {
        final protected String partitionName;

        public SyncPartitionOp(String partitionName) {
            queuer.super(ByteString.copyFromUtf8(partitionName));
            this.partitionName = partitionName;
        }
    }

    private final ServerConfiguration conf;
    private final InstanceConfig instanceConf;
    private HelixManager manager;
    private final TopicOpQueuer queuer;
    private final HedwigClient hubClient;
    private volatile RoutingTableProvider routingTable;
    private final Partitioner<ByteString> partitioner;

    // Partitions that I believe I am responsible for.
    protected final ConcurrentHashMap<String, TopicPartition> partitions = new ConcurrentHashMap<String, TopicPartition>();
    // Helix Persistence Manager
    protected final HelixPersistenceManager persistenceManager;
    // Local Subscription Manager
    protected final HelixLocalSubscriptionStorage localSubscriptionStore;
    // Topic Ownership Change Listeners
    protected final CopyOnWriteArraySet<TopicOwnershipChangeListener> listeners = new CopyOnWriteArraySet<TopicOwnershipChangeListener>();

    // my name
    protected final HedwigSocketAddress addr;
    protected final ByteString myName;

    public HelixTopicManager(ServerConfiguration conf, InstanceConfig instanceConf, OrderedSafeExecutor scheduler,
            OrderedSafeExecutor ioPool, HedwigHubClientFactory hubClientFactory, Partitioner<ByteString> partitioner)
            throws IOException {
        this.conf = conf;
        this.instanceConf = instanceConf;
        this.partitioner = partitioner;
        this.queuer = new TopicOpQueuer(scheduler);
        this.addr = conf.getServerAddr();
        this.myName = ByteString.copyFromUtf8(this.addr.toString().replaceAll("[.:]", "_"));
        // establish a client to local hedwig cluster, we will use local hedwig cluster
        // as a pub/sub commit destination
        this.hubClient = hubClientFactory.createNormalClient(addr);
        // prepare the local storage for persistence & subscription
        this.persistenceManager = new HelixPersistenceManager(conf, this, ioPool);
        this.localSubscriptionStore = new HelixLocalSubscriptionStorage(conf, ioPool);
    }

    InstanceConfig getInstanceConfig() {
        return instanceConf;
    }

    public HelixPersistenceManager getPersistenceManager() {
        return persistenceManager;
    }

    public HelixLocalSubscriptionStorage getLocalSubscriptionStorage() {
        return localSubscriptionStore;
    }

    public HedwigClient getHedwigHubClient() {
        return this.hubClient;
    }

    public void start() throws Exception {
        String clusterName = DEFAULT_CLUSTERNAME;
        manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceConf.getInstanceName(),
                InstanceType.PARTICIPANT, conf.getZkHost());
        manager.getStateMachineEngine().registerStateModelFactory(States.STATE_MODEL_NAME, this);
        manager.connect();
        // register the changes for the external view.
        routingTable = new RoutingTableProvider();
        manager.addExternalViewChangeListener(routingTable);
    }

    /**
     * Check routing table for the ownership of <partition, hedwigTopic>.
     *
     * @param partition
     *            partition name.
     * @param hedwigTopic
     *            hedwig topic name.
     * @param cb
     *            callback instance.
     * @param ctx
     *            callback context
     * @return true if this hub server is possible the owner for this topic,
     *         otherwise false.
     */
    private boolean checkRoutingTable(String partition, ByteString hedwigTopic, Callback<HedwigSocketAddress> cb,
            Object ctx) {
        if (null == routingTable) {
            // the topic manager is not ready to serve requests now. reject it
            // no leader found for this partition
            cb.operationFailed(
                    ctx,
                    PubSubException.create(StatusCode.SERVICE_DOWN, "Server " + addr + " is not ready."));
            return false;
        }
        List<InstanceConfig> instances = routingTable.getInstances(DEFAULT_NAMESPACE, partition, States.LEADER);
        if (instances.isEmpty()) {
            // no leader found for this partition
            cb.operationFailed(
                    ctx,
                    PubSubException.create(StatusCode.SERVICE_DOWN, "No leader found for partition " + partition
                            + " to serve topic " + hedwigTopic.toStringUtf8()));
            return false;
        } else if (!instances.get(0).equals(instanceConf)) {
            // not myself, redirect
            InstanceConfig ownerConfig = instances.get(0);
            HedwigSocketAddress owner = new HedwigSocketAddress(ownerConfig.getHostName(), Integer.parseInt(ownerConfig
                    .getPort()));
            if (logger.isDebugEnabled()) {
                logger.debug("Redirect request for topic {} to owner {}.", hedwigTopic.toStringUtf8(), owner);
            }
            cb.operationFinished(ctx, owner);
            return false;
        }
        return true;
    }

    @Override
    public void getOwner(final ByteString hedwigTopic, boolean shouldClaim, final Callback<HedwigSocketAddress> cb,
            Object ctx) {
        final String partition = partitioner.getPartition(hedwigTopic);
        // quick check this hub server owned partitions
        if (partitions.containsKey(partition)) {
            realGetOwner(partition, hedwigTopic, cb, ctx);
            return;
        }
        // check routing table, for those topics that are supposed not to be
        // owned by myself, redirect their requests quickly. no metadata
        // accesses for these requests actually, they just access local
        // cached routing table.
        if (!checkRoutingTable(partition, hedwigTopic, cb, ctx)) {
            return;
        }
        realGetOwner(partition, hedwigTopic, cb, ctx);
    }

    private void realGetOwner(final String partition, final ByteString hedwigTopic,
            final Callback<HedwigSocketAddress> originalCallback, Object context) {
        // it possible owned this topic, but might in bootstapped or fencing
        // state, we had to queue this requests until the partition is owned
        // but this hub server.
        this.enqueueAsyncPartitionOp(new AsyncPartitionOp<TopicPartition>(partition, new Callback<TopicPartition>() {
            @Override
            public void operationFailed(Object ctx, PubSubException e) {
                // check the routing table again
                if (checkRoutingTable(partition, hedwigTopic, originalCallback, ctx)) {
                    // tell us the hub is the ownership again. so it might be
                    // bad state like zookeeper session expire causing state
                    // changed, simply reject the requests to let client retry.
                    originalCallback.operationFailed(
                            ctx,
                            PubSubException.create(StatusCode.NOT_RESPONSIBLE_FOR_TOPIC, addr
                                    + " is suspicious to be the owner of partition " + partition + " for topic "
                                    + hedwigTopic.toStringUtf8()));
                    return;
                }
            }
            @Override
            public void operationFinished(Object ctx, TopicPartition topicPartition) {
                originalCallback.operationFinished(ctx, addr);
            }
        }, context) {
            @Override
            public void run() {
                TopicPartition tp = unsafeGetTopicPartition(partition);
                if (null == tp) {
                    logger.error("Failed to get partition {} for topic {}.", partition, hedwigTopic.toStringUtf8());
                    cb.operationFailed(
                            ctx,
                            PubSubException.create(StatusCode.NOT_RESPONSIBLE_FOR_TOPIC, "No partition " + partition
                                    + " found in " + addr + " for topic " + hedwigTopic.toStringUtf8()));
                    return;
                }
                if (tp.isLeader()) {
                    if (!tp.topics.contains(hedwigTopic)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug(
                                    "notify subscription & persistence manager to claim the ownership for topic {} : ",
                                    hedwigTopic.toStringUtf8());
                        }
                        notifyListenersAndAddToOwnedTopics(tp, hedwigTopic, cb, ctx);
                    } else {
                        cb.operationFinished(ctx, tp);
                    }
                    return;
                }
                // the topic is in other states
                cb.operationFailed(
                        ctx,
                        PubSubException.create(
                                StatusCode.NOT_RESPONSIBLE_FOR_TOPIC,
                                addr + " is not the leader of partition " + partition + " for topic "
                                        + hedwigTopic.toStringUtf8()));
                return;
            }
        });
    }

    @Override
    public void addTopicOwnershipChangeListener(TopicOwnershipChangeListener listener) {
        listeners.add(listener);
    }

    protected final synchronized void notifyListenersAndAddToOwnedTopics(final TopicPartition partition,
            final ByteString topic, final Callback<TopicPartition> originalCallback, final Object originalContext) {

        Callback<Void> postCb = new Callback<Void>() {

            @Override
            public void operationFinished(Object ctx, Void resultOfOperation) {
                partition.topics.add(topic);
                originalCallback.operationFinished(originalContext, partition);
            }

            @Override
            public void operationFailed(final Object ctx, final PubSubException exception) {
                Callback<Void> cb = new Callback<Void>() {
                    @Override
                    public void operationFinished(Object _ctx, Void _resultOfOperation) {
                        originalCallback.operationFailed(ctx, exception);
                    }

                    @Override
                    public void operationFailed(Object _ctx, PubSubException _exception) {
                        logger.error("Exception releasing topic", _exception);
                        originalCallback.operationFailed(ctx, exception);
                    }
                };
                realReleaseTopic(partition, topic, cb, originalContext);
            }
        };

        Callback<Void> mcb = CallbackUtils.multiCallback(listeners.size(), postCb, null);
        for (TopicOwnershipChangeListener listener : listeners) {
            // notify the listeners with the partition txn log.
            listener.acquiredTopic(topic, mcb, partition.txnLog);
        }
    }

    private void realReleaseTopic(TopicPartition partition, ByteString topic, Callback<Void> cb, Object ctx) {
        for (TopicOwnershipChangeListener listener : listeners) {
            listener.lostTopic(topic);
        }
        partition.topics.remove(topic);
        // complete releasing topic
        cb.operationFinished(ctx, null);
    }

    @Override
    public void releaseTopic(ByteString topic, Callback<Void> cb, Object ctx) {
        String partitionName = partitioner.getPartition(topic);
        enqueueAsyncPartitionOp(new ReleaseOp(partitionName, topic, cb, ctx));
    }

    private class ReleaseOp extends AsyncPartitionOp<Void> {

        final ByteString hedwigTopic;

        public ReleaseOp(String partitionName, ByteString hedwigTopic, Callback<Void> cb, Object ctx) {
            super(partitionName, cb, ctx);
            this.hedwigTopic = hedwigTopic;
        }

        @Override
        public void run() {
            TopicPartition tp = unsafeGetTopicPartition(partitionName);
            if (null == tp) {
                logger.error("Failed to get partition {} for topic {} to release topic ownership.", partitionName,
                        hedwigTopic.toStringUtf8());
                cb.operationFinished(ctx, null);
                return;
            }
            if (!tp.isLeader()) {
                cb.operationFinished(ctx, null);
                return;
            }
            realReleaseTopic(tp, hedwigTopic, cb, ctx);
        }

    }

    @Override
    public void stop() {
        // disconnect the manager first.
        manager.disconnect();
        logger.info("Disconnected helix manager.");
    }

    //
    // Topic Partitions Operations
    //

    @SuppressWarnings("rawtypes")
    protected void enqueueAsyncPartitionOp(AsyncPartitionOp op) {
        queuer.pushAndMaybeRun(ByteString.copyFromUtf8(op.partitionName), op);
    }

    protected void enqueueSyncPartitionOp(SyncPartitionOp op) {
        queuer.pushAndMaybeRun(ByteString.copyFromUtf8(op.partitionName), op);
    }

    public static ByteString partitionToHedwigTopic(String partition) {
        StringBuilder sb = new StringBuilder().append(".").append(partition);
        return ByteString.copyFromUtf8(sb.toString());
    }

    private TopicPartition unsafeGetTopicPartition(String partitionName) {
        return partitions.get(partitionName);
    }

    /**
     * State Transition: OFFLINE => STANDBY
     *
     * 1. subscriber to its partition topic.
     * 2. after subscribed, do a CATCHUP procedure to start delivery to receive
     * edits from it partition topic.
     *
     * The state is marked as BOOTSTRAPPED, only after finished the above two
     * operations. If it failed during this procedure, turn its state back to
     * OFFLINE and retry.
     */
    private class BecomeStandbyFromOfflineOp extends AsyncPartitionOp<TopicPartition> {
        public BecomeStandbyFromOfflineOp(String partitionName, Callback<TopicPartition> cb, Object ctx) {
            super(partitionName, cb, ctx);
        }

        @Override
        public void run() {
            final TopicPartition partition = unsafeGetTopicPartition(partitionName);
            if (null == partition) {
                logger.warn("No topic partition {} found to bootstrap, ignore it.", partitionName);
                // we don't need to callback since this is internal operation,
                // just ignore it.
                return;
            }
            if (!partition.canBoostrap()) {
                logger.error("TopicPartition {} can't be bootstrapped since it is in {} state.", partitionName,
                        partition.getState());
                // we don't need to callback if the topic partition in a
                // different state
                return;
            }
            partition.markBoostapping();
            if (logger.isDebugEnabled()) {
                logger.debug("OFFLINE -> STANDBY : reading partition data for {}.", partitionName);
            }
            partition.readPartitionData(new Callback<PartitionData>() {

                @Override
                public void operationFailed(Object ctx, PubSubException e) {
                    logger.error("Failed to read partition data from local storage for partition " + partitionName
                            + " : ", e);
                    partition.markOffline();
                    cb.operationFailed(ctx, e);
                }

                @Override
                public void operationFinished(Object ctx, PartitionData partitionData) {
                    if (null == partitionData) {
                        logger.info("OFFLINE -> STANDBY : no partition data found for partition {} in this node.",
                                partitionName);
                    } else {
                        partition.epoch = partitionData.getEpoch();
                    }
                    logger.info("OFFLINE -> STANDBY : finished reading partition data for {}, whose epoch is {}.",
                            partitionName, partition.epoch);
                    subscribe(partition);
                }

            }, ctx);
        }

        private void subscribe(final TopicPartition partition) {
            // TODO: we should differentiate different subscribe options.
            // since bootstrapping a new replica should copying data from other
            // replicas.
            SubscriptionOptions options = SubscriptionOptions.newBuilder()
                    .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).build();
            final ByteString hedwigTopic = partitionToHedwigTopic(partitionName);
            logger.info("OFFLINE -> STANDBY : partition {} subscribes to its transaction log topic {} as {}.",
                    va(partitionName, hedwigTopic.toStringUtf8(), addr.toString()));
            hubClient.getSubscriber().asyncSubscribe(hedwigTopic, myName, options, new Callback<Void>() {
                        @Override
                        public void operationFailed(Object ctx, PubSubException e) {
                            logger.error("OFFLINE -> STANDBY : TopicPartition " + partitionName
                                    + " failed to bootstrap itself by subscribing its partition topic : ", e);
                            partition.markOffline();
                            cb.operationFailed(ctx, e);
                        }

                        @Override
                        public void operationFinished(Object ctx, Void result) {
                            logger.info("OFFLINE -> STANDBY : TopicPartition {} finished bootstrapping itself.", partitionName);
                            catchup(hedwigTopic, myName, partition);
                        }
                    }, ctx);
        }

        private void catchup(ByteString hedwigTopic, ByteString hedwigSubscriber, TopicPartition partition) {
            // start deliver to catch up the local state.
            try {
                logger.info(
                        "OFFLINE -> STANDBY : Partition {} started to receive edits from transaction log to catch up.",
                        partitionName);
                hubClient.getSubscriber().startDelivery(hedwigTopic, hedwigSubscriber, partition);
                partition.markBoostapped();
                cb.operationFinished(ctx, partition);
            } catch (ClientNotSubscribedException e) {
                partition.markOffline();
                cb.operationFailed(ctx, e);
            } catch (AlreadyStartDeliveryException e) {
                // it means someone already started delivery, simply return
                partition.markBoostapped();
                cb.operationFinished(ctx, partition);
            }
        }
    }

    private void doBecomeStandbyFromOffline(final String topicPartition) {
        enqueueAsyncPartitionOp(new BecomeStandbyFromOfflineOp(topicPartition,
                new Callback<TopicPartition>() {
                    @Override
                    public void operationFailed(Object ctx, PubSubException e) {
                        // tried to become standby again
                        logger.info("Tried to become standby for topic partition {} again.", topicPartition);
                        doBecomeStandbyFromOffline(topicPartition);
                    }
                    @Override
                    public void operationFinished(Object ctx, TopicPartition partition) {
                        // become standby
                        logger.info("{} become standby for topic partition {}.", myName.toStringUtf8(), topicPartition);
                    }
                }, null));
    }

    @Override
    public void becomeStandbyFromOffline(String topicPartition) {
        // insert topic partition object into it.
        TopicPartition partition = partitions.get(topicPartition);
        if (null == partition) {
            partition = new TopicPartition(topicPartition);
            TopicPartition oldPartition = partitions.putIfAbsent(topicPartition, partition);
            if (null == oldPartition) {
                doBecomeStandbyFromOffline(topicPartition);
            }
        }
    }

    /**
     * State Transition: STANDBY => LEADER
     *
     * 1. subscriber to its partition topic.
     * 2. after subscribed, do a CATCHUP procedure to start delivery to receive
     * edits from it partition topic.
     *
     * The state is marked as BOOTSTRAPPED, only after finished the above two
     * operations. If it failed during this procedure, turn its state back to
     * OFFLINE and retry.
     */
    private class BecomeLeaderFromStandbyOp extends AsyncPartitionOp<TopicPartition> {
        final long epoch;
        public BecomeLeaderFromStandbyOp(String partitionName, long epoch, Callback<TopicPartition> cb, Object ctx) {
            super(partitionName, cb, ctx);
            this.epoch = epoch;
        }

        @Override
        public void run() {
            final TopicPartition partition = unsafeGetTopicPartition(partitionName);
            if (null == partition) {
                logger.warn("No topic partition {} found to fence, ignore it.", partitionName);
                // we don't need to callback since this is internal operation,
                // just ignore it.
                return;
            }
            if (!partition.canFence()) {
                logger.error("TopicPartition {} can't be fenced since it is in {} state.", partitionName,
                        partition.getState());
                // we don't need to callback if the topic partition in a
                // different state
                return;
            }
            partition.markFencing();
            TopicPartitionTxnLog partitionLog = new TopicPartitionTxnLog(epoch, partitionName);
            partition.setTxnLog(partitionLog);
            IFenceTxn.Builder ifenceTxn = IFenceTxn.newBuilder().setHostname(addr.toString());
            InternalTxn.Builder itxnBuilder = InternalTxn.newBuilder().setType(InternalTxn.Type.FENCE).setFenceTxn(ifenceTxn);
            logger.info("STANDBY -> LEADER : fencing partition {} at epoch {}.", partitionName, epoch);
            partitionLog.processTxn(itxnBuilder, new Callback<Void>() {
                @Override
                public void operationFailed(Object context, PubSubException e) {
                    // TODO Auto-generated method stub
                    logger.error("TopicPartition " + partitionName
                            + " failed to publish fence message to fence partition : ", e);
                    partition.setTxnLog(null);
                    partition.markBoostapped();
                    cb.operationFailed(ctx, e);
                }
                @Override
                public void operationFinished(Object context, Void result) {
                    partition.markFenced();
                    logger.info("STANDBY -> LEADER : fenced partition {} at epoch {}.", partitionName, epoch);
                    cb.operationFinished(ctx, partition);
                }
            });
        }

    }

    private void doBecomeLeaderFromStandby(final String topicPartition, final long epoch) {
        enqueueAsyncPartitionOp(new BecomeLeaderFromStandbyOp(topicPartition,
                epoch, new Callback<TopicPartition>() {
                    @Override
                    public void operationFailed(Object ctx, PubSubException e) {
                        // tried to become standby again
                        logger.info("Tried to become leader for topic partition {} again.", topicPartition);
                        doBecomeLeaderFromStandby(topicPartition, epoch);
                    }
                    @Override
                    public void operationFinished(Object ctx, TopicPartition partition) {
                        // become standby
                        logger.info("{} become leader for topic partition {}.", myName.toStringUtf8(), topicPartition);
                    }
                }, null));
    }

    @Override
    public void becomeLeaderFromStandby(String topicPartition, long epoch) {
        doBecomeLeaderFromStandby(topicPartition, epoch);
    }

    /**
     * State Transition: LEADER => STANDBY
     *
     * 1. released all it already owned topics.
     * 2. marked the partition back to BOOTSTRAPPED.
     */
    private class BecomeStandyFromLeaderOp extends SyncPartitionOp {

        public BecomeStandyFromLeaderOp(String partitionName) {
            super(partitionName);
        }

        @Override
        protected void runInternal() {
            final TopicPartition partition = unsafeGetTopicPartition(partitionName);
            if (null == partition) {
                logger.warn("No topic partition {} found to downgrade to standby, ignore it.", partitionName);
                // we don't need to callback since this is internal operation,
                // just ignore it.
                return;
            }
            // release all the topic ownership.
            partition.releaseTopics();
            // marked partition as Bootstrapped, so we would reject publish
            // requests.
            partition.markBoostapped();
        }
    }

    @Override
    public void becomeStandbyFromLeader(String topicPartition) {
        enqueueSyncPartitionOp(new BecomeStandyFromLeaderOp(topicPartition));
    }

    /**
     * State Transition: STANDBY => OFFLINE
     *
     * 1. close subscription to its partition topic.
     */
    private class BecomeOfflineFromStandbyOp extends AsyncPartitionOp<TopicPartition> {
        public BecomeOfflineFromStandbyOp(String partitionName, Callback<TopicPartition> cb, Object ctx) {
            super(partitionName, cb, ctx);
        }

        @Override
        public void run() {
            final TopicPartition partition = unsafeGetTopicPartition(partitionName);
            if (null == partition) {
                logger.warn("No topic partition {} found to offline, ignore it.", partitionName);
                // we don't need to callback since this is internal operation,
                // just ignore it.
                return;
            }
            if (!partition.canOffline()) {
                logger.error("TopicPartition {} can't be bootstrapped since it is in {} state.", partitionName,
                        partition.getState());
                // we don't need to callback if the topic partition in a
                // different state
                return;
            }
            final ByteString hedwigTopic = partitionToHedwigTopic(partitionName);
            partition.markOffline();
            closeSubscription(hedwigTopic, myName, partition);
        }

        private void closeSubscription(final ByteString hedwigTopic, final ByteString hedwigSubscriber,
                final TopicPartition partition) {
            // TODO: for now, we just close the subscription. after we supported
            // partition move, we might consider unsubscribe the partition topic
            // and clear the local state.
            hubClient.getSubscriber().asyncCloseSubscription(hedwigTopic, hedwigSubscriber, new Callback<Void>() {
                @Override
                public void operationFailed(Object ctx, PubSubException e) {
                    logger.error("TopicPartition " + partitionName
                            + " failed to be offline by closing subscription to its partition topic : ", e);
                    closeSubscription(hedwigTopic, hedwigSubscriber, partition);
                }

                @Override
                public void operationFinished(Object ctx, Void result) {
                    logger.info("TopicPartition {} has become offline.", partitionName);
                    cb.operationFinished(ctx, partition);
                }
            }, ctx);
        }
    }

    @Override
    public void becomeOfflineFromStandby(final String topicPartition) {
        enqueueAsyncPartitionOp(new BecomeOfflineFromStandbyOp(topicPartition, new Callback<TopicPartition>() {
            @Override
            public void operationFailed(Object ctx, PubSubException e) {
                // it would not be called.
                logger.error("Unexpected state found when become offline from standby for topic partition : "
                        + topicPartition, e);
            }
            @Override
            public void operationFinished(Object ctx, TopicPartition partition) {
                // remove the partition from partition map
                partitions.remove(topicPartition, partition);
            }
        }, null));
    }

    @Override
    public TopicStateModel createNewStateModel(String partition) {
        return new TopicStateModel(instanceConf.getInstanceName(), partition, this);
    }

}
