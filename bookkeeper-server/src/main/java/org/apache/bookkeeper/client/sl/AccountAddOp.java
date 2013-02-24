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
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.AddCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.RequestCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AddDataEntryRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AddDataEntryRequest.PayLoad;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AddDataEntryResponse;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.OperationType;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.PacketHeader;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SimpleRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.util.SuperLedgerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * This represents a pending add operation. When it has got success from all
 * bookies, it sees if its at the head of the pending adds queue, and if yes,
 * sends ack back to the application.
 */
public class AccountAddOp extends AccountOp implements RequestCallback<AddDataEntryResponse> {

    final static Logger LOG = LoggerFactory.getLogger(AccountAddOp.class);

    // request context
    final byte[] data;
    final int offset;
    final int length;
    final AddCallback cb;
    final Object ctx;
    long entryId;
    boolean recoverAdd;

    // request state
    Set<Integer> writeSet;
    final DistributionSchedule.AckSet ackSet;

    enum State {
        PENDING, COMPLETED, FENCED
    }
    State state = State.PENDING;

    // request body
    final AddDataEntryRequest.Builder addReq;

    AccountAddOp(AccounterImpl acct, byte[] data, int offset, int length, AddCallback cb, Object ctx) {
        super(acct);
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.cb = cb;
        this.ctx = ctx;
        this.entryId = BookieProtocol.INVALID_ENTRY_ID;

        ackSet = acct.getDistributionSchedule().getAckSet();
        addReq = AddDataEntryRequest.newBuilder().setAcctName(acct.acctName).setMasterKey(acct.lh.ledgerKey);
    }

    AccountAddOp initiate(long entryId, ByteBuffer buf) {
        this.entryId = entryId;
        writeSet = new HashSet<Integer>(acct.getDistributionSchedule().getWriteSet(entryId));
        if (addReq.getPayloadsCount() > 0) {
            addReq.getPayloadsBuilder(0).setBody(ByteString.copyFrom(buf));
            addReq.getPayloadsBuilder(0).setEntryId(entryId);
        } else {
            PayLoad.Builder payload = PayLoad.newBuilder().setBody(ByteString.copyFrom(buf))
                    .setEntryId(entryId);
            addReq.addPayloads(payload);
        }
        return this;
    }

    AccountAddOp enableRecoveryAdd(long entryId) {
        recoverAdd = true;
        this.entryId = entryId;
        addReq.setFlag(AddDataEntryRequest.Flag.RECOVERY_ADD);
        return this;
    }

    void fence() {
        state = State.FENCED;
    }

    void sendWriteRequest(int bookieIndex) {
        PacketHeader.Builder header = PacketHeader.newBuilder().setType(OperationType.ADD_DATA_ENTRY);
        SimpleRequest.Builder sr = SimpleRequest.newBuilder().setHeader(header).setAddEntryRequest(addReq);
        acct.lh.slClient.sendRequest(acct.lh.metadata.ensemble.get(bookieIndex), acct.lh.ledgerName,
                SuperLedgerUtils.epoch(entryId), acct.orderKey, sr, this, bookieIndex);
    }

    void unsetSuccessAndSendWriteRequest(int bookieIndex) {
        if (State.FENCED == state || State.COMPLETED == state) {
            return;
        }
        // the unset doesn't happen on the write set, so we don't need to resend
        // the write request. but we had to try to send success add callbacks.
        // because there might be entries already completed but wait for
        // ensemble
        // change completed.
        // E.g.
        // ensemble (A, B, C, D), entry k is written to (A, B, D). An ensemble
        // change happened to replace C with E. so entry k could not complete
        // until C is replaced by E successfully. when ensemble change finished,
        // it tried to unset entry k. but C is not in k's write set, so no entry
        // is written again, no one trigger #sendAddSuccessCallbacks. so k would
        // never complete.
        //
        // So sendAddSuccessCallback when unset an entry as below.
        if (!writeSet.contains(bookieIndex)) {
            acct.sendAddSuccessCallbacks();
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Unsetting success for " + acct.getName() + ", entry: " + entryId + " bookie index: "
                    + bookieIndex);
        }

        // if we had already heard a success from this array index, need to
        // increment our number of responses that are pending, since we are
        // going to unset this success
        ackSet.removeBookie(bookieIndex);
        state = State.PENDING;

        sendWriteRequest(bookieIndex);
    }

    @Override
    public void requestComplete(StatusCode code, ByteString ledgerName, AddDataEntryResponse response,
            InetSocketAddress addr, Object ctx) {
        int bookieIndex = (Integer) ctx;

        // if state is changed, ignore the response.
        if ((null != response && response.getEntriesCount() > 0 && entryId != response.getEntries(0))
                || State.FENCED == state) {
            return;
        }

        switch (code) {
        case EOK:
            // continue;
            break;
        case EFENCED:
            int epoch = SuperLedgerUtils.epoch(entryId);
            LOG.warn("Fencing exception on write (" + acct.getName() + ", entry:" + entryId + ") @ epoch "
                    + epoch);
            if (epoch < acct.lh.epoch) {
                fence();
                acct.sendAddSuccessCallbacks();
            } else {
                // some other one fence the ledger, we could not handle it
                acct.lh.handleUnrecoverableError();
            }
            return;
        case EUA:
            LOG.warn("Unauthorized access exception on write (" + acct.getName() + ", entry:" + entryId
                    + ") @ epoch " + SuperLedgerUtils.epoch(entryId));
            acct.lh.handleUnrecoverableError();
            return;
        default:
            LOG.warn("Write did not succeed on (" + acct.getName() + ", entry:" + entryId + ") @ epoch "
                    + SuperLedgerUtils.epoch(entryId));
            // TODO: we don't replace bookie when bookie failed, simply retry it
            // (maybe we need some backoff mechanism).
            // TODO: improve failure handling. E.g. hint handle off.
            this.unsetSuccessAndSendWriteRequest(bookieIndex);
            return;
        }

        if (!writeSet.contains(bookieIndex)) {
            LOG.warn("Received a response for ({}, eid:{}) from {}@{}, but it doesn't belong to {}.",
                    new Object[] { acct.getName(), entryId, addr, bookieIndex, writeSet });
            return;
        }
        if (ackSet.addBookieAndCheck(bookieIndex) && State.COMPLETED != state) {
            state = State.COMPLETED;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Complete ({}, eid:{}).", new Object[] { acct.getName(), entryId });
            }
            // when completed an entry, try to send success add callbacks in
            // order
            acct.sendAddSuccessCallbacks();
        }
    }

    void submitCallback(final StatusCode code) {
        cb.addComplete(code, acct, entryId, ctx);
    }

    void failCallback(final StatusCode code) {
        cb.addComplete(code, acct, entryId, ctx);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PendingAddOp(").append(acct.getName()).append(", eid:").append(entryId).append(", state:")
                .append(state).append(")");
        return sb.toString();
    }

    //
    // Execute the Add operation
    //

    @Override
    void executeOp() {
        acct.initiateAddOp(this, data, offset, length);
        state = State.PENDING;
        for (int bookieIndex : writeSet) {
            sendWriteRequest(bookieIndex);
        }
    }

}
