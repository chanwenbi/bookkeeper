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

import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.ReadMetadataCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.RequestCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AccountMetadata;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.OperationType;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.PacketHeader;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.ReadAcctMetaRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.ReadAcctMetaResponse;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SimpleRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.util.SuperLedgerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Read Account Metadata. Since the metadata in account is not critical enough,
 * which are just to reduce potential duplicate accesses. so we just try the
 * best to read the latest version of the metadata.
 */
class AccountMetadataReadOp extends AccountOp implements RequestCallback<ReadAcctMetaResponse> {

    static final Logger LOG = LoggerFactory.getLogger(AccountMetadataReadOp.class);

    // request context
    final ReadMetadataCallback cb;
    final Object ctx;

    // request state
    int numResponsesPending;
    AccountMetadata lastSeenMetadata;
    long lastEntryId = BookieProtocol.INVALID_ENTRY_ID;
    boolean completed = false;
    final DistributionSchedule.QuorumCoverageSet coverageSet;

    // request body
    final ReadAcctMetaRequest.Builder readReq;

    AccountMetadataReadOp(AccounterImpl acct, ReadMetadataCallback cb, Object ctx) {
        super(acct);
        this.cb = cb;
        this.ctx = ctx;
        this.coverageSet = acct.lh.distributionSchedule.getCoverageSet();
        this.readReq = ReadAcctMetaRequest.newBuilder().setAcctName(acct.acctName).setReadLastEntryId(true);
        this.numResponsesPending = acct.lh.metadata.getEnsembleSize();
    }

    void sendReadTo(int bookieIndex) {
        PacketHeader.Builder header = PacketHeader.newBuilder().setType(OperationType.READ_ACCT_META);
        SimpleRequest.Builder request = SimpleRequest.newBuilder().setHeader(header)
                .setReadAcctRequest(readReq);
        acct.lh.slClient.sendRequest(acct.lh.metadata.ensemble.get(bookieIndex), acct.lh.ledgerName,
                acct.lh.epoch, acct.orderKey, request, this, bookieIndex);
    }

    @Override
    void failCallback(StatusCode code) {
        cb.readComplete(code, acct, null, lastEntryId, ctx);
    }

    @Override
    public void requestComplete(StatusCode code, ByteString ledgerName, ReadAcctMetaResponse response,
            InetSocketAddress addr, Object ctx) {
        if (completed) {
            return;
        }
        int bookieIndex = (Integer) ctx;
        --numResponsesPending;
        if (StatusCode.EUA == code) {
            completed = true;
            failCallback(code);
            return;
        }
        boolean heardValidResponse = false;
        // Check the metadata response.
        if (StatusCode.EOK == code) {
            AccountMetadata metadata = response.getAcctMeta();
            if (null != lastSeenMetadata) {
                if (metadata.getVersion() > lastSeenMetadata.getVersion()) {
                    lastSeenMetadata = metadata;
                }
            } else {
                lastSeenMetadata = metadata;
            }
            if (response.hasLastEntryId() && response.getLastEntryId() > lastEntryId) {
                lastEntryId = response.getLastEntryId();
            }
            heardValidResponse = true;
        } else if (SuperLedgerUtils.isNoEntryException(code)) {
            heardValidResponse = true;
        }
        if (heardValidResponse && coverageSet.addBookieAndCheckCovered(bookieIndex)) {
            completed = true;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Read account metadata for {} with enough valid responses completed.",
                        acct.getName());
            }
            cb.readComplete(null == lastSeenMetadata ? StatusCode.ENOACCT : StatusCode.EOK, acct,
                    lastSeenMetadata, lastEntryId, this.ctx);
            return;
        }
        if (0 == numResponsesPending) {
            // have got all responses back but was still not enought, just fail
            // the operation
            LOG.error("Failed to read account metadata for {}", acct.getName());
            cb.readComplete(StatusCode.EREADEXCEPTION, acct, null, lastEntryId, this.ctx);
            return;
        }
    }

    @Override
    void executeOp() {
        for (int i = 0; i < acct.lh.metadata.ensemble.size(); i++) {
            sendReadTo(i);
        }
    }

}
