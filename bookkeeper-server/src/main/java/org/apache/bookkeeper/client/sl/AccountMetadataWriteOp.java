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
import java.util.HashSet;
import java.util.Set;

import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.RequestCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.WriteMetadataCallback;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AccountMetadata;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.OperationType;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.PacketHeader;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SimpleRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.WriteAcctMetaRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.WriteAcctMetaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * This represents a metadata write operation for an account.
 */
class AccountMetadataWriteOp extends AccountOp implements RequestCallback<WriteAcctMetaResponse> {

    final static Logger LOG = LoggerFactory.getLogger(AccountMetadataWriteOp.class);

    // request context
    final WriteMetadataCallback cb;
    final Object ctx;
    // request state
    final Set<Integer> writeSet;
    final DistributionSchedule.AckSet ackSet;
    boolean completed = false;
    // request body
    final WriteAcctMetaRequest.Builder writeReq;

    AccountMetadataWriteOp(AccounterImpl acct, AccountMetadata.Builder metadata,
            WriteMetadataCallback cb, Object ctx) {
        super(acct);
        this.cb = cb;
        this.ctx = ctx;

        ackSet = acct.getDistributionSchedule().getAckSet();
        writeSet = new HashSet<Integer>(acct.getDistributionSchedule().getWriteSet(acct.acctName.hashCode()));
        writeReq = WriteAcctMetaRequest.newBuilder().setAcctName(acct.acctName).setAcctMeta(metadata)
                .setMasterKey(acct.lh.ledgerKey);
    }

    void sendWriteRequest(int bookieIndex) {
        PacketHeader.Builder header = PacketHeader.newBuilder().setType(OperationType.WRITE_ACCT_META);
        SimpleRequest.Builder sr = SimpleRequest.newBuilder().setHeader(header).setWriteAcctRequest(writeReq);
        acct.lh.slClient.sendRequest(acct.lh.metadata.ensemble.get(bookieIndex), acct.lh.ledgerName,
                acct.lh.epoch, acct.orderKey, sr, this, bookieIndex);
    }

    void unsetSuccessAndSendWriteRequest(int bookieIndex) {
        if (completed) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Unsetting success for metadata write on {}, bookie index: {}.", acct.getName(),
                    bookieIndex);
        }
        ackSet.removeBookie(bookieIndex);
        completed = false;
        // TODO: do we need kind of backoff mechanism?
        sendWriteRequest(bookieIndex);
    }

    @Override
    public void requestComplete(StatusCode code, ByteString ledgerName, WriteAcctMetaResponse response,
            InetSocketAddress addr, Object ctx) {
        int bookieIndex = (Integer) ctx;

        switch (code) {
        case EOK:
            // continue;
            break;
        case EFENCED:
            LOG.warn("Fencing exception on write metadata for {} @ epoch {}.", acct.getName(), acct.lh.epoch);
            if (!completed) {
                completed = true;
                submitCallback(StatusCode.EFENCED);
            }
            return;
        case EUA:
            LOG.warn("Unauthorized access exception on write metadata for {}.", acct.getName());
            acct.lh.handleUnrecoverableError();
            return;
        default:
            LOG.warn("Write metadata did not succeed on {}.", acct.getName());
            unsetSuccessAndSendWriteRequest(bookieIndex);
            return;
        }
        if (!writeSet.contains(bookieIndex)) {
            LOG.warn("Received a metadata response for {} from {}@{}, but it doesn't belong to {}.",
                    new Object[] { acct.getName(), addr, bookieIndex, writeSet });
            return;
        }
        if (ackSet.addBookieAndCheck(bookieIndex) && !completed) {
            completed = true;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Complete metadata write for {}", acct.getName());
            }
            submitCallback(StatusCode.EOK);
        }
    }

    void submitCallback(final StatusCode code) {
        cb.writeComplete(code, acct, ctx);
    }

    @Override
    void failCallback(final StatusCode code) {
        cb.writeComplete(code, acct, ctx);
    }

    @Override
    void executeOp() {
        for (int bookieIndex : writeSet) {
            sendWriteRequest(bookieIndex);
        }
    }
}
