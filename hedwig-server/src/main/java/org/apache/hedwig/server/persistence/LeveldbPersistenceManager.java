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
package org.apache.hedwig.server.persistence;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ServerNotResponsibleForTopicException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protoextensions.MessageIdUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.persistence.ScanCallback.ReasonForFinish;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TopicOwnershipChangeListener;
import org.apache.hedwig.util.Callback;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This persistence manager uses leveldb to store messages.
 *
 * TODO: 1) collect metrics; 2) benchmark it
 */
public class LeveldbPersistenceManager implements PersistenceManagerWithRangeScan, TopicOwnershipChangeListener {

    static Logger logger = LoggerFactory.getLogger(LeveldbPersistenceManager.class);

    private static final long INVALID_SEQ_ID = 0L;
    private static final int UNLIMITED = 0;
    private static final byte SEP = '\001';
    private static final int NUM_DELETIONS_PER_BATCH = 32;
    private static final int MSG_KEY_SUFFIX_LEN = 1 + Long.SIZE;
    private static final MessageSeqId START_SEQ_ID = MessageSeqId.newBuilder().setLocalComponent(0L).build();

    protected class TopicInfo implements Runnable {
        final ByteString topic;
        volatile MessageSeqId lastSeqIdPushed = START_SEQ_ID;
        volatile long consumedSeqId = 0;
        long seqIdUntilDeleted = 0;
        volatile int messageBound = UNLIMITED;
        // context object passed from topic manager when acquired topic
        volatile Object topicContext;

        TopicInfo(ByteString topic, Object topicContext) {
            this.topic = topic;
            this.topicContext = topicContext;
        }

        public Object getTopicContext() {
            return this.topicContext;
        }

        public TopicInfo setTopicContext(Object topicContext) {
            this.topicContext = topicContext;
            return this;
        }

        public void setLastSeqIdPushed(MessageSeqId lastSeqIdPushed) {
            if (lastSeqIdPushed.getLocalComponent() > this.lastSeqIdPushed.getLocalComponent()) {
                this.lastSeqIdPushed = lastSeqIdPushed;
            }
        }

        void takeFirstSeqIdAsConsumeSeqId(long seqId) {
            if (seqId == INVALID_SEQ_ID) {
                return;
            }
            long priorSeqId = seqId - 1;
            if (priorSeqId > consumedSeqId) {
                consumedSeqId = priorSeqId;
                seqIdUntilDeleted = consumedSeqId;
            } else if (priorSeqId != seqIdUntilDeleted) {
                seqIdUntilDeleted = priorSeqId;
            }
            if (seqIdUntilDeleted < consumedSeqId) {
                // schedule a delete operation
                consumeWorker.submit(this);
            }
        }

        void takeLastSeqIdAsLastPushedSeqId(long seqId) {
            if (lastSeqIdPushed == null) {
                lastSeqIdPushed = MessageSeqId.newBuilder().setLocalComponent(seqId).build();
            } else {
                if (lastSeqIdPushed.getLocalComponent() < seqId) {
                    lastSeqIdPushed = MessageSeqId.newBuilder()
                            .addAllRemoteComponents(lastSeqIdPushed.getRemoteComponentsList()).setLocalComponent(seqId)
                            .build();
                }
            }
        }

        public MessageSeqId buildNextMessageSeqId(Message requestedMsg) {
            long localSeqId = lastSeqIdPushed.getLocalComponent() + 1;
            MessageSeqId.Builder builder = MessageSeqId.newBuilder();
            if (requestedMsg.hasMsgId()) {
                MessageIdUtils.takeRegionMaximum(builder, lastSeqIdPushed, requestedMsg.getMsgId());
            } else {
                builder.addAllRemoteComponents(lastSeqIdPushed.getRemoteComponentsList());
            }
            builder.setLocalComponent(localSeqId);
            return builder.build();
        }

        public Message buildMessage(Message msg, MessageSeqId seqId) {
            return Message.newBuilder(msg).setMsgId(seqId).build();
        }

        byte[] serialize() {
            byte[] seqidData = lastSeqIdPushed.toByteArray();
            byte[] data = new byte[Long.SIZE + seqidData.length];
            long2Bytes(consumedSeqId, data, 0);
            System.arraycopy(seqidData, 0, data, Long.SIZE, seqidData.length);
            return data;
        }

        TopicInfo deserialize(ByteString topic, byte[] data) throws InvalidProtocolBufferException {
            this.consumedSeqId = bytes2Long(data, 0);
            this.seqIdUntilDeleted = this.consumedSeqId;
            this.lastSeqIdPushed = MessageSeqId
                    .parseFrom(ByteString.copyFrom(data, Long.SIZE, data.length - Long.SIZE));
            return this;
        }

        @Override
        public void run() {
            if (seqIdUntilDeleted == consumedSeqId) {
                return;
            }
            // clean-up consumed entries
            WriteBatch batch = null;
            try {
                batch = msgDB.createWriteBatch();
                int numBatch = 0;
                long seq = seqIdUntilDeleted + 1;
                for (; seq <= consumedSeqId; ++seq) {
                    batch.delete(getMessageKey(topic, seq));
                    ++numBatch;
                    if (numBatch >= NUM_DELETIONS_PER_BATCH) {
                        msgDB.write(batch);
                        batch.close();
                        seqIdUntilDeleted = seq;
                        numBatch = 0;
                        batch = msgDB.createWriteBatch();
                    }
                }
                if (numBatch > 0) {
                    msgDB.write(batch);
                    seqIdUntilDeleted = seq - 1;
                }
                dumpTopicInfo(topic, this);
            } catch (DBException dbe) {
                logger.warn("Failed to delete consumed entries for topic " + topic.toStringUtf8() + " : ", dbe);
            } catch (IOException ie) {
                logger.warn("Failed to delete consumed entries for topic " + topic.toStringUtf8() + " : ", ie);
            } finally {
                if (null != batch) {
                    try {
                        batch.close();
                    } catch (IOException ioe) {
                    }
                }
            }
        }
    }

    protected final OrderedSafeExecutor workerPool;
    private final ExecutorService consumeWorker;

    protected final ConcurrentMap<ByteString, TopicInfo> topicInfos;
    protected final DB msgDB;
    protected final ReadOptions ro;
    protected final WriteOptions wo;
    protected final Options dbOptions;

    public LeveldbPersistenceManager(ServerConfiguration cfg, TopicManager tm, OrderedSafeExecutor ioPool)
            throws IOException {
        topicInfos = new ConcurrentHashMap<ByteString, TopicInfo>();

        String dbPath = cfg.getLeveldbPersistencePath();
        File dbDir = new File(dbPath);

        dbOptions = new Options();
        dbOptions.blockSize(cfg.getLeveldbBlockSize());
        dbOptions.cacheSize(cfg.getLeveldbCacheSize());
        dbOptions.compressionType(CompressionType.SNAPPY);
        dbOptions.createIfMissing(true);
        dbOptions.writeBufferSize(cfg.getLeveldbWriteBufferSize());
        dbOptions.maxOpenFiles(cfg.getLeveldbMaxOpenFiles());

        try {
            msgDB = factory.open(dbDir, dbOptions);
        } catch (DBException dbe) {
            throw new IOException("Failed to open leveldb at " + dbPath, dbe);
        }

        // initialize read/write options
        ro = new ReadOptions();
        // we don't need to fill cache for normal reads/scans
        // since after read/scan, the messages are in ReadAheadCache
        // we don't need to cache them again.
        ro.fillCache(false);
        wo = new WriteOptions();

        workerPool = ioPool;
        consumeWorker = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Leveldb-Consume-Worker-Thread");
            }
        });

        // preload db
        try {
            initialize();
        } catch (DBException dbe) {
            throw new IOException("Failed to initialize leveldb : ", dbe);
        }
        // register itself to topic ownership manager after db is initialized
        tm.addTopicOwnershipChangeListener(this);

        logger.info("Leveldb persistence info started using directory {}.", dbPath);
    }

    @Override
    public void stop() {
        workerPool.shutdown();
        consumeWorker.shutdown();
    }

    protected void submitByTopic(ByteString topic, SafeRunnable r) {
        workerPool.submitOrdered(topic, r);
    }

    protected void submitByPartition(String partitionName, SafeRunnable r) {
        workerPool.submitOrdered(partitionName, r);
    }

    private byte[] getMessageKey(ByteString topic, long seqid) {
        byte[] key = new byte[topic.size() + MSG_KEY_SUFFIX_LEN];
        topic.copyTo(key, 0);
        key[topic.size()] = SEP;
        long2Bytes(seqid, key, topic.size() + 1);
        return key;
    }

    private byte[] getTopicKey(ByteString topic) {
        return topic.toByteArray();
    }

    /**
     * Tricky: check the reversed 6th byte. If it was a <code>\001</code>,
     * it is a message key, otherwise, it is a topic key.
     */
    private boolean isMessageKey(byte[] key) {
        if (key.length <= MSG_KEY_SUFFIX_LEN) {
            return false;
        }
        return SEP == key[key.length - MSG_KEY_SUFFIX_LEN];
    }

    private ByteString getTopicFromMessageKey(byte[] key) {
        return ByteString.copyFrom(key, 0, key.length - MSG_KEY_SUFFIX_LEN);
    }

    private long getSeqIdFromMessageKey(byte[] key) {
        return bytes2Long(key, key.length - MSG_KEY_SUFFIX_LEN + 1);
    }

    private ByteString getTopicFromTopicKey(byte[] key) {
        return ByteString.copyFrom(key);
    }

    private boolean isSameTopic(byte[] topic, byte[] key) {
        return 0 == compareBytes(topic, 0, topic.length, key, 0, topic.length);
    }

    private void dumpTopicInfo(ByteString topic, TopicInfo topicInfo) throws DBException {
        msgDB.put(getTopicKey(topic), topicInfo.serialize());
    }

    private void initialize() throws DBException {
        logger.info("Initializing leveldb persistence manager.");
        // preload topic states and warmup db files
        DBIterator iter = null;
        try {
            iter = msgDB.iterator(ro);
            iter.seekToFirst();

            long numTopics = 0;

            if (!iter.hasNext()) {
                logger.info("Finished initialized leveldb persistence manager : {} topics.", numTopics);
                return;
            }
            Entry<byte[], byte[]> entry = iter.next();
            while (null != entry) {
                byte[] key = entry.getKey();
                byte[] data = entry.getValue();
                boolean isMsgKey = isMessageKey(key);
                TopicInfo ti;
                long curSeqId;
                if (isMsgKey) {
                    ti = new TopicInfo(getTopicFromMessageKey(key), null);
                    // no topic info, set consume seq id to the id prior to
                    // first id
                    curSeqId = getSeqIdFromMessageKey(key);
                    ti.consumedSeqId = ti.seqIdUntilDeleted = curSeqId - 1;
                } else {
                    ti = new TopicInfo(getTopicFromTopicKey(key), null);
                    try {
                        ti.deserialize(ti.topic, data);
                    } catch (InvalidProtocolBufferException e) {
                        logger.warn("Failed to deserialize topic info for topic " + ti.topic.toStringUtf8(), e);
                    }
                    curSeqId = INVALID_SEQ_ID;
                }
                entry = initializeTopicInfo(ti, curSeqId, iter);
                ++numTopics;
                topicInfos.put(ti.topic, ti);
                if (numTopics % 10000 == 0) {
                    logger.info("Preloaded {} topics.", numTopics);
                }
            }
            logger.info("Finished initialized leveldb persistence manager : {} topics.", numTopics);
        } finally {
            if (null != iter) {
                try {
                    iter.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * Initialize currrent topic info and return next topic info. return null if
     * there is no entries.
     */
    private Entry<byte[], byte[]> initializeTopicInfo(TopicInfo curTopicInfo, long lastSeqId, DBIterator iter)
            throws DBException {
        byte[] topicBytes = curTopicInfo.topic.toByteArray();
        boolean foundNewTopic = false;
        long firstSeqId = lastSeqId;
        Entry<byte[], byte[]> entry = null;
        byte[] key = null;
        while (iter.hasNext()) {
            entry = iter.next();
            key = entry.getKey();
            if (isSameTopic(topicBytes, key)) {
                lastSeqId = getSeqIdFromMessageKey(key);
                if (firstSeqId == INVALID_SEQ_ID) {
                    firstSeqId = lastSeqId;
                }
            } else {
                foundNewTopic = true;
                break;
            }
        }
        curTopicInfo.takeLastSeqIdAsLastPushedSeqId(lastSeqId);
        curTopicInfo.takeFirstSeqIdAsConsumeSeqId(firstSeqId);
        if (!foundNewTopic) {
            return null;
        }
        return entry;
    }

    @Override
    public void acquiredTopic(ByteString topic, Callback<Void> callback, Object ctx) {
        if (logger.isDebugEnabled()) {
            logger.debug("{} trying to acquire topic {}.", getClass().getName(), topic.toStringUtf8());
        }
        submitByTopic(topic, new AcquireOp(topic, callback, ctx));
    }

    class AcquireOp extends SafeRunnable {
        final ByteString topic;
        final Callback<Void> cb;
        final Object ctx;

        AcquireOp(ByteString topic, Callback<Void> cb, Object ctx) {
            this.topic = topic;
            this.cb = cb;
            this.ctx = ctx;
        }

        @Override
        public void safeRun() {
            TopicInfo ti = topicInfos.get(topic);
            if (null != ti) {
                ti.setTopicContext(ctx);
                // Already acquired, do nothing
                // most of acquire would return quickly, since we preload when
                // initialize
                cb.operationFinished(ctx, null);
                return;
            }
            // only hit here when a topic is released and acquire again.
            DBIterator iter = null;
            try {
                iter = msgDB.iterator();
                iter.seek(getTopicKey(topic));
                ti = new TopicInfo(topic, ctx);
                if (iter.hasNext()) {
                    Entry<byte[], byte[]> entry = iter.next();
                    byte[] key = entry.getKey();
                    byte[] data = entry.getValue();
                    boolean isMsgKey = isMessageKey(key);
                    long curSeqId;
                    if (isMsgKey) {
                        // no topic info, set consume seq id to the id prior to
                        // first id
                        curSeqId = getSeqIdFromMessageKey(key);
                        ti.consumedSeqId = ti.seqIdUntilDeleted = curSeqId - 1;
                    } else {
                        try {
                            ti.deserialize(topic, data);
                        } catch (InvalidProtocolBufferException e) {
                            logger.warn("Failed to deserialize topic info for topic " + topic.toStringUtf8(), e);
                        }
                        curSeqId = INVALID_SEQ_ID;
                    }
                    initializeTopicInfo(ti, curSeqId, iter);
                }
                topicInfos.put(topic, ti);
                if (logger.isDebugEnabled()) {
                    logger.debug("{} claimed ownership for topic {}.", getClass().getName(), topic.toStringUtf8());
                }
                cb.operationFinished(ctx, null);
            } catch (DBException dbe) {
                cb.operationFailed(ctx, new PubSubException.ServiceDownException(dbe));
                return;
            } finally {
                if (null != iter) {
                    try {
                        iter.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
    }

    @Override
    public void lostTopic(ByteString topic) {
        submitByTopic(topic, new ReleaseOp(topic));
    }

    protected class ReleaseOp extends SafeRunnable {

        protected final ByteString topic;

        public ReleaseOp(ByteString topic) {
            this.topic = topic;
        }

        @Override
        public void safeRun() {
            TopicInfo topicInfo = topicInfos.remove(topic);
            if (topicInfo == null) {
                return;
            }
            // update topic state to msg db
            try {
                dumpTopicInfo(topic, topicInfo);
            } catch (DBException dbe) {
                // we could not
                logger.error("Failed to release topic ownership for topic " + topic.toStringUtf8(), dbe);
            }
        }
    }

    @Override
    public void persistMessage(PersistRequest request) {
        submitByTopic(request.topic, new PersistOp(request));
    }

    protected void doPersistMessage(ByteString topic, MessageSeqId msgId, Message msg,
            Callback<MessageSeqId> callback, Object ctx) {
        // put the persist request to queue
        try {
            msgDB.put(getMessageKey(topic, msgId.getLocalComponent()), msg.toByteArray());
            callback.operationFinished(ctx, msgId);
            return;
        } catch (DBException dbe) {
            callback.operationFailed(ctx, new PubSubException.ServiceDownException(dbe));
            // we don't need to give up the topic, since the seqid only
            // changed
            // after a message is persisted.
            return;
        }
    }

    class PersistOp extends SafeRunnable {

        final PersistRequest request;
        final ByteString topic;

        PersistOp(PersistRequest request) {
            this.topic = request.topic;
            this.request = request;
        }

        @Override
        public void safeRun() {
            final TopicInfo topicInfo = topicInfos.get(request.topic);
            if (null == topicInfo) {
                request.getCallback().operationFailed(request.ctx,
                        new PubSubException.ServerNotResponsibleForTopicException(""));
                return;
            }
            MessageSeqId msgId = topicInfo.buildNextMessageSeqId(request.message);
            Message msgToSerialize = topicInfo.buildMessage(request.message, msgId);
            topicInfo.lastSeqIdPushed = msgId;
            doPersistMessage(topic, msgId, msgToSerialize, request.getCallback(), request.getCtx());
        }
    }

    @Override
    public void scanSingleMessage(ScanRequest request) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void scanMessages(RangeScanRequest request) {
        submitByTopic(request.topic, new RangeScanOp(request));
    }

    class RangeScanOp extends SafeRunnable {

        RangeScanRequest request;
        ByteString topic;

        RangeScanOp(RangeScanRequest request) {
            this.request = request;
            this.topic = request.topic;
        }

        @Override
        public void safeRun() {
            TopicInfo topicInfo = topicInfos.get(topic);
            if (null == topicInfo) {
                request.callback.scanFailed(request.ctx, new PubSubException.ServerNotResponsibleForTopicException(""));
                return;
            }
            startReadingFrom(request.startSeqId, topicInfo.lastSeqIdPushed.getLocalComponent());
        }

        private void issueEmptyMsgs(long ssid, long esid) {
            // no entries any more
            for (long seqid = ssid; seqid <= esid; ++seqid) {
                issueEmptyMsg(seqid);
            }
        }

        private void issueEmptyMsg(long seqid) {
            // callback with a empty string.
            MessageSeqId.Builder msgIdBuilder = MessageSeqId.newBuilder().setLocalComponent(seqid);
            Message msg = Message.newBuilder().setMsgId(msgIdBuilder).setBody(ByteString.EMPTY).build();
            request.callback.messageScanned(request.ctx, msg);
        }

        void startReadingFrom(long startSeqId, long endSeqId) {
            long correctedEndSeqId = Math.min(startSeqId + request.messageLimit - 1, endSeqId);

            if (startSeqId > correctedEndSeqId) {
                request.callback.scanFinished(request.ctx, ReasonForFinish.NO_MORE_MESSAGES);
                return;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Issuing a leveldb scan for topic {} from seqid: {} to seqid: {}.", new Object[] { topic,
                        startSeqId, correctedEndSeqId });
            }

            DBIterator iter = null;
            long seqid = startSeqId;
            byte[] topicBytes = topic.toByteArray();
            try {
                iter = msgDB.iterator();
                iter.seek(getMessageKey(topic, startSeqId));
                while (iter.hasNext()) {
                    Entry<byte[], byte[]> entry = iter.next();
                    byte[] key = entry.getKey();
                    byte[] value = entry.getValue();
                    if (!isSameTopic(topicBytes, key)) {
                        break;
                    }
                    long readSeqid = getSeqIdFromMessageKey(key);
                    if (readSeqid == seqid) {
                        Message msg;
                        try {
                            msg = Message.parseFrom(value);
                        } catch (IOException e) {
                            logger.error("Unreadable message found for topic: " + topic.toStringUtf8() + ", seqid: "
                                    + seqid + " : ", e);
                            // issue an empty message instead of hanging here,
                            // since data already corrupted.
                            issueEmptyMsg(seqid);
                            return;
                        }
                        request.callback.messageScanned(request.ctx, msg);
                        ++seqid;
                    } else if (readSeqid > seqid) {
                        long endSeqidToSkip = Math.min(readSeqid - 1, correctedEndSeqId);
                        issueEmptyMsgs(seqid, endSeqidToSkip);
                        seqid = endSeqidToSkip + 1;
                    }
                    if (seqid > correctedEndSeqId) {
                        break;
                    }
                }
                issueEmptyMsgs(seqid, correctedEndSeqId);
                request.callback.scanFinished(request.ctx, ReasonForFinish.NO_MORE_MESSAGES);
                return;
            } catch (DBException dbe) {
                logger.error("Error while scanning topic " + topic.toStringUtf8() + " : ", dbe);
                request.callback.scanFailed(request.ctx, new PubSubException.ServiceDownException(dbe));
                return;
            } finally {
                if (null != iter) {
                    try {
                        iter.close();
                    } catch (IOException ie) {
                        logger.warn("Failed to close leveldb iterator : ", ie);
                    }
                }
            }
        }
    }

    @Override
    public void deliveredUntil(ByteString topic, Long seqId) {
        // Nothing to do here.
    }

    private long getMinSeqIdForTopic(TopicInfo topicInfo) {
        if (topicInfo == null || topicInfo.messageBound == UNLIMITED) {
            return Long.MIN_VALUE;
        } else {
            return (topicInfo.lastSeqIdPushed.getLocalComponent() - topicInfo.messageBound) + 1;
        }
    }

    class ConsumeUntilOp extends SafeRunnable {
        private final long seqId;
        final ByteString topic;

        public ConsumeUntilOp(ByteString topic, long seqId) {
            this.topic = topic;
            this.seqId = seqId;
        }

        @Override
        public void safeRun() {
            TopicInfo topicInfo = topicInfos.get(topic);
            if (topicInfo == null) {
                return;
            }
            if (topicInfo.consumedSeqId >= seqId) {
                return;
            }
            topicInfo.consumedSeqId = seqId;
            consumeWorker.submit(topicInfo);
        }
    }

    @Override
    public void consumedUntil(ByteString topic, Long seqId) {
        submitByTopic(topic, new ConsumeUntilOp(topic, Math.max(seqId, getMinSeqIdForTopic(topicInfos.get(topic)))));
    }

    @Override
    public void consumeToBound(ByteString topic) {
        TopicInfo topicInfo = topicInfos.get(topic);
        if (topicInfo == null || topicInfo.messageBound == UNLIMITED) {
            return;
        }
        submitByTopic(topic, new ConsumeUntilOp(topic, getMinSeqIdForTopic(topicInfo)));
    }

    @Override
    public MessageSeqId getCurrentSeqIdForTopic(ByteString topic) throws ServerNotResponsibleForTopicException {
        TopicInfo topicInfo = topicInfos.get(topic);
        if (null == topicInfo) {
            throw new PubSubException.ServerNotResponsibleForTopicException("");
        }
        // TODO: if it was used in-memory mode, it should return the last
        // consumed seq id
        // return
        // MessageSeqId.newBuilder().setLocalComponent(topicInfo.consumeSeqId).build();
        return topicInfo.lastSeqIdPushed;
    }

    @Override
    public long getSeqIdAfterSkipping(ByteString topic, long seqId, int skipAmount) {
        return Math.max(seqId + skipAmount, getMinSeqIdForTopic(topicInfos.get(topic)));
    }

    @Override
    public void setMessageBound(ByteString topic, Integer bound) {
        TopicInfo topicInfo = topicInfos.get(topic);
        if (null != topicInfo) {
            topicInfo.messageBound = bound;
        }
    }

    @Override
    public void clearMessageBound(ByteString topic) {
        setMessageBound(topic, UNLIMITED);
    }

    /**
     * the caller should ensure <code>b</code> has enough space to hold a
     * <i>long</i> numer.
     */
    private static void long2Bytes(long val, byte[] b, int offset) {
        for (int i = Long.SIZE - 1; i > 0; i--) {
            b[offset + i] = (byte) val;
            val >>>= 8;
        }
        b[offset] = (byte) val;
    }

    private static long bytes2Long(byte[] b, int offset) {
        long l = 0;
        for (int i = offset; i < offset + Long.SIZE; i++) {
            l <<= 8;
            l ^= b[i] & 0xFF;
        }
        return l;
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
