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

import java.io.IOException;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.IDataTxn;
import org.apache.hedwig.protocol.PubSubProtocol.InternalTxn;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protoextensions.MessageIdUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.persistence.PersistRequest;
import org.apache.hedwig.server.topics.helix.HelixTopicManager;
import org.apache.hedwig.util.Callback;

import com.google.protobuf.ByteString;

/**
 * A {@link HelixLocalPersistenceStorage} backed persistence manager.
 */
public class HelixPersistenceManager extends HelixLocalPersistenceStorage {

    public HelixPersistenceManager(ServerConfiguration cfg, HelixTopicManager tm, OrderedSafeExecutor ioPool)
            throws IOException {
        super(cfg, tm, ioPool);
    }

    class HelixTopicInfo extends TopicInfo {

        volatile MessageSeqId lastSeqIdPublished = null;

        HelixTopicInfo(ByteString topic, Object topicContext) {
            super(topic, topicContext);
        }

        MessageSeqId getLastPublishedSeqId() {
            if (null == lastSeqIdPublished) {
                lastSeqIdPublished = lastSeqIdPushed;
            }
            return lastSeqIdPublished;
        }

        void setLastPublishedSeqId(MessageSeqId seqid) {
            if (seqid.getLocalComponent() > lastSeqIdPublished.getLocalComponent()) {
                this.lastSeqIdPublished = seqid;
            }
        }

    }

    class HelixPersistOp extends SafeRunnable {
        final PersistRequest request;
        final ByteString topic;

        HelixPersistOp(PersistRequest request) {
            this.topic = request.getTopic();
            this.request = request;
        }

        @Override
        public void safeRun() {
            final HelixTopicInfo topicInfo = (HelixTopicInfo) topicInfos.get(topic);
            final Object topicContext;
            if (null == topicInfo || !((topicContext = topicInfo.getTopicContext()) instanceof TxnLog)) {
                request.getCallback().operationFailed(request.getCtx(),
                        PubSubException.create(StatusCode.NOT_RESPONSIBLE_FOR_TOPIC, ""));
                return;
            }
            final MessageSeqId msgId = topicInfo.buildNextMessageSeqId(request.getMessage(),
                    topicInfo.getLastPublishedSeqId());
            Message msgToSerialize = topicInfo.buildMessage(request.getMessage(), msgId);
            topicInfo.setLastPublishedSeqId(msgId);
            if (logger.isDebugEnabled()) {
                logger.debug("Publish message {} to partition log for topic {}",
                        MessageIdUtils.msgIdToReadableString(msgId), topic.toStringUtf8());
            }
            TxnLog txnLog = (TxnLog) topicContext;
            IDataTxn.Builder dataTxn = IDataTxn.newBuilder().setTopic(topic).setBody(msgToSerialize.toByteString());
            InternalTxn.Builder txn = InternalTxn.newBuilder().setDataTxn(dataTxn).setType(InternalTxn.Type.DATA);
            txnLog.processTxn(txn, new Callback<Void>() {
                @Override
                public void operationFailed(Object ctx, PubSubException e) {
                    request.getCallback().operationFailed(request.getCtx(), e);
                }

                @Override
                public void operationFinished(Object ctx, Void result) {
                    request.getCallback().operationFinished(request.getCtx(), msgId);
                }

            });
        }
    }

    @Override
    protected TopicInfo createTopicInfo(ByteString topic, Object ctx) {
        return new HelixTopicInfo(topic, ctx);
    }

    @Override
    public void persistMessage(PersistRequest request) {
        submitByTopic(request.getTopic(), new HelixPersistOp(request));
    }

}
