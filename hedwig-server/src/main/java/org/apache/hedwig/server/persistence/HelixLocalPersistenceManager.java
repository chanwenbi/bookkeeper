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

import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PartitionData;
import org.apache.hedwig.util.Callback;

import com.google.protobuf.ByteString;

/**
 * Interface providing local persistence manager for Helix based pub/sub
 * storage.
 */
public interface HelixLocalPersistenceManager extends PersistenceManagerWithRangeScan {

    /**
     * Persist message locally using given <i>seqId</i>
     *
     * @param topic
     *            Topic name.
     * @param seqId
     *            Message seq id.
     * @param msg
     *            Message
     * @param callback
     *            Callback
     * @param ctx
     *            Ctx
     */
    public void persistMessage(ByteString topic, MessageSeqId seqId, Message msg, Callback<MessageSeqId> callback,
            Object ctx);

    /**
     * Update {@link PartitionData} for a given partition <i>partitionName</i>.
     *
     * @param partitionName
     *            Partition Name.
     * @param partitionData
     *            Partition Dta.
     * @param callback
     *            Callback instance.
     * @param ctx
     *            Callback context.
     */
    public void updatePartitionData(String partitionName, PartitionData partitionData, Callback<Void> callback,
            Object ctx);

    /**
     * Read {@link PartitionData} for a given partition <i>partitionName</i>.
     *
     * @param partitionName
     *            Partition Name.
     * @param callback
     *            Callback instance.
     * @param ctx
     *            Callback context.
     */
    public void readPartitionData(String partitionName, Callback<PartitionData> callback, Object ctx);
}
