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

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PartitionData;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.persistence.HelixLocalPersistenceManager;
import org.apache.hedwig.server.persistence.LeveldbPersistenceManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.util.Callback;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class HelixLocalPersistenceStorage extends LeveldbPersistenceManager implements HelixLocalPersistenceManager {

    static Logger logger = LoggerFactory.getLogger(HelixLocalPersistenceStorage.class);

    final DB partitionDB;
    final ReadOptions pDbRo;
    final WriteOptions pDbWo;
    final Options pDbOptions;

    public HelixLocalPersistenceStorage(ServerConfiguration cfg, TopicManager tm, OrderedSafeExecutor executor) throws IOException {
        super(cfg, tm, executor);
        String pDbPath = cfg.getLeveldbPartitionDBPath();
        File dbDir = new File(pDbPath);

        pDbOptions = new Options();
        pDbOptions.blockSize(cfg.getLeveldbBlockSize());
        pDbOptions.cacheSize(cfg.getLeveldbPartitionDBCacheSize());
        pDbOptions.compressionType(CompressionType.SNAPPY);
        pDbOptions.createIfMissing(true);
        pDbOptions.writeBufferSize(cfg.getLeveldbPartitionDBWriteBufferSize());
        pDbOptions.maxOpenFiles(cfg.getLeveldbMaxOpenFiles());

        try {
            partitionDB = factory.open(dbDir, pDbOptions);
        } catch (DBException dbe) {
            throw new IOException("Failed to open partition db at " + pDbPath, dbe);
        }
        // initialize read/write options
        pDbRo = new ReadOptions();
        pDbRo.fillCache(true);
        pDbWo = new WriteOptions();
        logger.info("Leveldb partition db is serving at directory {}.", pDbPath);
    }

    @Override
    public void stop() {
        try {
            partitionDB.close();
        } catch (IOException e) {
            logger.error("Failed to close subscription database : ", e);
        }
        super.stop();
    }

    class LocalPersistOp extends SafeRunnable {

        final ByteString topic;
        final MessageSeqId msgId;
        final Message msg;
        final Callback<MessageSeqId> callback;
        final Object ctx;

        LocalPersistOp(ByteString topic, MessageSeqId msgId, Message msg, Callback<MessageSeqId> callback, Object ctx) {
            this.topic = topic;
            this.msgId = msgId;
            this.msg = msg;
            this.callback = callback;
            this.ctx = ctx;
        }

        @Override
        public void safeRun() {
            doPersistMessage(topic, msgId, msg, callback, ctx);
        }

    }

    @Override
    public void persistMessage(ByteString topic, MessageSeqId seqId, Message msg, Callback<MessageSeqId> callback,
            Object ctx) {
        submitByTopic(topic, new LocalPersistOp(topic, seqId, msg, callback, ctx));
    }

    @Override
    public void updatePartitionData(final String partitionName, final PartitionData partitionData,
            final Callback<Void> callback, final Object ctx) {
        submitByPartition(partitionName, new SafeRunnable() {

            @Override
            public void safeRun() {
                try {
                    partitionDB.put(partitionName.getBytes(), partitionData.toByteArray(), pDbWo);
                    callback.operationFinished(ctx, null);
                } catch (DBException dbe) {
                    callback.operationFailed(ctx, PubSubException.create(StatusCode.SERVICE_DOWN,
                            "Could not persist partition data for partition " + partitionName));
                    return;
                }
            }

        });
    }

    @Override
    public void readPartitionData(final String partitionName, final Callback<PartitionData> callback, final Object ctx) {
        submitByPartition(partitionName, new SafeRunnable() {
            @Override
            public void safeRun() {
                try {
                    byte[] data = partitionDB.get(partitionName.getBytes());
                    if (null == data) {
                        callback.operationFinished(ctx, null);
                        return;
                    }
                    try {
                        PartitionData pData = PartitionData.parseFrom(data);
                        callback.operationFinished(ctx, pData);
                    } catch (InvalidProtocolBufferException ipbe) {
                        logger.error("Invalid partition data read for partition " + partitionName + " : ", ipbe);
                        // just simply return null data.
                        callback.operationFinished(ctx, null);
                        return;
                    }
                } catch (DBException dbe) {
                    callback.operationFailed(ctx, PubSubException.create(StatusCode.SERVICE_DOWN,
                            "Could not read partition data for partition " + partitionName));
                    return;
                }
            }

        });
    }

}
