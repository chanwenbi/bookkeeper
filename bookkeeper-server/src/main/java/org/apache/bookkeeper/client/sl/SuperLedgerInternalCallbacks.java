/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.client.sl;

import java.net.InetSocketAddress;

import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AccountMetadata;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.jboss.netty.buffer.ChannelBuffer;

import com.google.protobuf.ByteString;

/**
 * Declaration of the callback interfaces used for super ledger, but not exposed
 * to the client application.
 */
public class SuperLedgerInternalCallbacks {

    public static interface RequestCallback<T> {
        public void requestComplete(StatusCode code, ByteString ledgerName, T response,
                InetSocketAddress addr, Object ctx);
    }

    public static interface AddEntryCallback {
        public void addEntryComplete(int rc, ByteString ledgerName, ByteString acctName, long entryId,
                InetSocketAddress addr, Object ctx);
    }

    public static interface ReadEntryCallback {
        public void readEntryComplete(int rc, ByteString ledgerName, ByteString acctName, long entryId,
                ChannelBuffer buffer, Object ctx);
    }

    public static interface WriteMetadataCallback {
        public void writeComplete(StatusCode code, Accounter acct, Object ctx);
    }

    public static interface ReadMetadataCallback {
        public void readComplete(StatusCode code, Accounter acct, AccountMetadata acctMetadata,
                long lastEntryId, Object ctx);
    }

    public static interface FenceSuperLedgerCallback {
        public void fenceComplete(StatusCode code, SuperLedgerHandle ledgerHandle, Object ctx);
    }

    public static interface ReconfigureCallback {
        public void reconfigureComplete(StatusCode code, SuperLedgerHandle ledgerHandle, Object ctx);
    }

}
