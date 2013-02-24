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
package org.apache.bookkeeper.meta;

import java.io.Closeable;

import org.apache.bookkeeper.client.sl.SuperLedgerMetadata;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;

import com.google.protobuf.ByteString;

/**
 * SuperLedgerManager takes responsibility of super ledger management in client
 * side.
 */
public interface SuperLedgerManager extends Closeable {
    
    public static interface SuperLedgerMetadataCallback<T> {
        public void complete(StatusCode code, T value);
    }

    /**
     * Write SuperLedger Metadata.
     * 
     * @param ledgerName
     *            Super ledger name.
     * @param metadata
     *            Super ledger metadata to write
     * @param callback
     *            Callback when finished writing ledger metadata.
     */
    public void writeSuperLedgerMetadata(ByteString ledgerName, SuperLedgerMetadata metadata,
            SuperLedgerMetadataCallback<Void> callback);

    /**
     * Read SuperLedger Metadata.
     * 
     * @param ledgerName
     *            Super Ledger Name.
     * @param callback
     *            Callback when finished reading ledger metadata.
     */
    public void readSuperLedgerMetadata(ByteString ledgerName,
            SuperLedgerMetadataCallback<SuperLedgerMetadata> callback);
}
