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
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.FenceSuperLedgerCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.RequestCallback;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.FenceSuperLedgerRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.FenceSuperLedgerResponse;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.OperationType;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.PacketHeader;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SimpleRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Fence the super ledger.
 */
class SuperLedgerFenceOp extends SafeRunnable implements RequestCallback<FenceSuperLedgerResponse> {

    static final Logger LOG = LoggerFactory.getLogger(SuperLedgerFenceOp.class);

    private final SuperLedgerHandle slh;
    private final FenceSuperLedgerCallback cb;
    private final Object ctx;
    private final int epoch;

    private final FenceSuperLedgerRequest.Builder fenceReq;
    final DistributionSchedule.QuorumCoverageSet coverageSet;

    int numResponsesPending;
    volatile boolean completed = false;

    SuperLedgerFenceOp(SuperLedgerHandle slh, int epoch, FenceSuperLedgerCallback cb, Object ctx) {
        this.slh = slh;
        this.cb = cb;
        this.ctx = ctx;
        this.epoch = epoch;
        this.numResponsesPending = slh.metadata.getEnsembleSize();
        this.coverageSet = slh.distributionSchedule.getCoverageSet();
        this.fenceReq = FenceSuperLedgerRequest.newBuilder().setMasterKey(slh.ledgerKey).setEpoch(epoch);
    }

    void sendReadTo(int bookieIndex) {
        PacketHeader.Builder header = PacketHeader.newBuilder().setType(OperationType.FENCE_SUPER_LEDGER);
        SimpleRequest.Builder request = SimpleRequest.newBuilder().setHeader(header)
                .setFenceRequest(fenceReq);
        slh.slClient.sendRequest(slh.metadata.ensemble.get(bookieIndex), slh.ledgerName, epoch,
                slh.ledgerName.hashCode(), request, this, bookieIndex);
    }

    @Override
    public void requestComplete(StatusCode code, ByteString ledgerName, FenceSuperLedgerResponse response,
            InetSocketAddress addr, Object ctx) {
        if (completed) {
            return;
        }
        int bookieIndex = (Integer) ctx;
        numResponsesPending--;
        if (StatusCode.EUA == code) {
            cb.fenceComplete(code, slh, this.ctx);
            completed = true;
            return;
        }
        boolean heardValidResponse = StatusCode.EOK == code || StatusCode.ENOSUPERLEDGER == code;
        if (heardValidResponse && coverageSet.addBookieAndCheckCovered(bookieIndex)) {
            completed = true;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Fence super ledger {} with enough valid responses completed.", slh.ledgerName.toStringUtf8());
            }
            cb.fenceComplete(StatusCode.EOK, slh, this.ctx);
            return;
        }
        if (0 == numResponsesPending) {
            // Have got all the responses back but was still not enough, just
            // fail the operation
            LOG.error("Failed to fence super ledger {} which didn't hear success responses from all quorums",
                    slh.ledgerName.toStringUtf8());
            cb.fenceComplete(StatusCode.EFENCEEXCEPTION, slh, this.ctx);
            return;
        }
    }

    @Override
    public void safeRun() {
        for (int i = 0; i < slh.metadata.ensemble.size(); i++) {
            sendReadTo(i);
        }
    }

}
