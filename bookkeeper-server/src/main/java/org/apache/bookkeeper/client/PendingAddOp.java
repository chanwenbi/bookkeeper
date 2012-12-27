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
package org.apache.bookkeeper.client;

import java.util.HashSet;
import java.util.Set;
import java.net.InetSocketAddress;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * This represents a pending add operation. When it has got success from all
 * bookies, it sees if its at the head of the pending adds queue, and if yes,
 * sends ack back to the application. If a bookie fails, a replacement is made
 * and placed at the same position in the ensemble. The pending adds are then
 * rereplicated.
 *
 *
 */
class PendingAddOp implements WriteCallback {
    final static Logger LOG = LoggerFactory.getLogger(PendingAddOp.class);

    ChannelBuffer toSend;
    AddCallback cb;
    Object ctx;
    long entryId;
    Set<Integer> writeSet;

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;

    LedgerHandle lh;
    boolean isRecoveryAdd = false;

    PendingAddOp(LedgerHandle lh, AddCallback cb, Object ctx) {
        this.lh = lh;
        this.cb = cb;
        this.ctx = ctx;
        this.entryId = LedgerHandle.INVALID_ENTRY_ID;
        
        ackSet = lh.distributionSchedule.getAckSet();
    }

    /**
     * Enable the recovery add flag for this operation.
     * @see LedgerHandle#asyncRecoveryAddEntry
     */
    PendingAddOp enableRecoveryAdd() {
        isRecoveryAdd = true;
        return this;
    }

    void setEntryId(long entryId) {
        this.entryId = entryId;
        writeSet = new HashSet<Integer>(lh.distributionSchedule.getWriteSet(entryId));
    }

    void sendWriteRequest(int bookieIndex) {
        int flags = isRecoveryAdd ? BookieProtocol.FLAG_RECOVERY_ADD : BookieProtocol.FLAG_NONE;

        lh.bk.bookieClient.addEntry(lh.metadata.currentEnsemble.get(bookieIndex), lh.ledgerId, lh.ledgerKey, entryId, toSend,
                this, bookieIndex, flags);
    }

    void unsetSuccessAndSendWriteRequest(int bookieIndex) {
        if (toSend == null) {
            // this addOp hasn't yet had its mac computed. When the mac is
            // computed, its write requests will be sent, so no need to send it
            // now
            return;
        }
        // the unset doesn't happen on the write set, so we don't need to resend
        // the write request. but we had to try to send success add callbacks.
        // because there might be entries already completed but wait for ensemble
        // change completed.
        // E.g.
        // ensemble (A, B, C, D), entry k is written to (A, B, D). An ensemble change
        // happened to replace C with E. so entry k could not complete until C is
        // replaced by E successfully. when ensemble change finished, it tried to
        // unset entry k. but C is not in k's write set, so no entry is written again,
        // no one trigger #sendAddSuccessCallbacks. so k would never complete.
        //
        // So sendAddSuccessCallback when unset an entry as below.
        if (!writeSet.contains(bookieIndex)) {
            lh.sendAddSuccessCallbacks();
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Unsetting success for ledger: " + lh.ledgerId + " entry: " + entryId + " bookie index: "
                      + bookieIndex);
        }

        // if we had already heard a success from this array index, need to
        // increment our number of responses that are pending, since we are
        // going to unset this success
        ackSet.removeBookie(bookieIndex);
        completed = false;

        sendWriteRequest(bookieIndex);
    }

    void initiate(ChannelBuffer toSend) {
        this.toSend = toSend;
        for (int bookieIndex : writeSet) {
            sendWriteRequest(bookieIndex);
        }
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx) {
        int bookieIndex = (Integer) ctx;


        switch (rc) {
        case BKException.Code.OK:
            // continue
            break;
        case BKException.Code.LedgerFencedException:
            LOG.warn("Fencing exception on write: " + ledgerId + ", " + entryId);
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        case BKException.Code.UnauthorizedAccessException:
            LOG.warn("Unauthorized access exception on write: " + ledgerId + ", " + entryId);
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        default:
            LOG.warn("Write did not succeed: " + ledgerId + ", " + entryId);
            lh.handleBookieFailure(addr, bookieIndex);
            return;
        }

        if (!writeSet.contains(bookieIndex)) {
            LOG.warn("Received a response for (lid:{}, eid:{}) from {}@{}, but it doesn't belong to {}.",
                     new Object[] { ledgerId, entryId, addr, bookieIndex, writeSet });
            return;
        }

        if (ackSet.addBookieAndCheck(bookieIndex) && !completed) {
            completed = true;

            LOG.debug("Complete (lid:{}, eid:{}).", ledgerId, entryId);
            // when completed an entry, try to send success add callbacks in order
            lh.sendAddSuccessCallbacks();
        }
    }

    void submitCallback(final int rc) {
        cb.addComplete(rc, lh, entryId, ctx);
        lh.opCounterSem.release();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PendingAddOp(lid:").append(lh.ledgerId)
          .append(", eid:").append(entryId).append(", completed:")
          .append(completed).append(")");
        return sb.toString();
    }

}
