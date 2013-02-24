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

import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.DigestManager.RecoveryData;
import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.RequestCallback;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.OperationType;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.PacketHeader;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.ReadDataEntryRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.ReadDataEntryResponse;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SimpleRequest;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.util.SuperLedgerUtils;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

class EpochReadLastConfirmedOp extends AccountOp implements RequestCallback<ReadDataEntryResponse> {

    static final Logger LOG = LoggerFactory.getLogger(EpochReadLastConfirmedOp.class);

    int numResponsesPending;
    RecoveryData maxRecoveredData;
    volatile boolean completed = false;

    final int epoch;
    final long entryId;
    final EpochLastConfirmedDataCallback cb;
    final DistributionSchedule.QuorumCoverageSet coverageSet;
    final ReadDataEntryRequest.Builder readReq;

    static interface EpochLastConfirmedDataCallback {
        void readEpochLastConfirmedDataComplete(StatusCode code, RecoveryData data);
    }

    /**
     * Callback to get all recovered data from the request.
     */
    EpochReadLastConfirmedOp(AccounterImpl acct, int epoch, EpochLastConfirmedDataCallback callback) {
        super(acct);
        this.cb = callback;
        this.epoch = epoch;
        this.numResponsesPending = acct.lh.metadata.getEnsembleSize();
        this.coverageSet = acct.lh.distributionSchedule.getCoverageSet();
        this.entryId = SuperLedgerUtils.entryid(epoch, 0);
        this.maxRecoveredData = new RecoveryData(entryId, 0);
        this.readReq = ReadDataEntryRequest.newBuilder().setAcctName(acct.acctName).setEntryId(entryId);
    }

    @Override
    void failCallback(StatusCode code) {
        this.cb.readEpochLastConfirmedDataComplete(code, null);
    }

    @Override
    void executeOp() {
        for (int i = 0; i < acct.lh.metadata.ensemble.size(); i++) {
            sendReadTo(i);
        }
    }

    private void sendReadTo(int bookieIndex) {
        PacketHeader.Builder header = PacketHeader.newBuilder();
        header.setType(OperationType.READ_DATA_ENTRY);
        SimpleRequest.Builder request = SimpleRequest.newBuilder().setHeader(header)
                .setReadEntryRequest(readReq);
        acct.lh.slClient.sendRequest(acct.lh.metadata.ensemble.get(bookieIndex), acct.lh.ledgerName, epoch,
                acct.orderKey, request, this, bookieIndex);
    }

    @Override
    public void requestComplete(StatusCode code, ByteString ledgerName, ReadDataEntryResponse response,
            InetSocketAddress addr, Object ctx) {
        if (completed) {
            return;
        }
        int bookieIndex = (Integer) ctx;
        numResponsesPending--;
        if (StatusCode.EUA == code) {
            acct.lh.handleUnrecoverableError();
            completed = true;
            return;
        }
        boolean heardValidResponse = false;
        if (StatusCode.EOK == code) {
            ByteString data = response.getData();
            try {
                RecoveryData recoveryData = acct.lh.macManager.verifyDigestAndReturnLastConfirmed(ChannelBuffers.wrappedBuffer(data.toByteArray()));
                if (recoveryData.lastAddConfirmed > maxRecoveredData.lastAddConfirmed) {
                    maxRecoveredData = recoveryData;
                }
                heardValidResponse = true;
            } catch (BKDigestMatchException e) {
                // Too bad, this bookie didn't give us a valid answer, we
                // still might be able to recover though so continue
                LOG.error("Mac mismatch for account: " + acct + ", entry: " + entryId
                        + " while reading last entry from bookie: "
                        + acct.lh.metadata.ensemble.get(bookieIndex));
            }
        }
        if (SuperLedgerUtils.isNoEntryException(code)) {
            // this still counts as a valid response, e.g., if the client
            // crashed without writing any entry
            heardValidResponse = true;
        }
        if (heardValidResponse && coverageSet.addBookieAndCheckCovered(bookieIndex)) {
            completed = true;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Read last confirmed entry for {} with enough valid responses completed.",
                        acct.getName());
            }
            cb.readEpochLastConfirmedDataComplete(StatusCode.EOK, maxRecoveredData);
            return;
        }
        if (0 == numResponsesPending) {
            // Have got all the responses back but was still not enough, just
            // fail the operation
            LOG.error(
                    "Failed to read last confirmed entry for {} which didn't hear success responses from all quorums",
                    acct);
            cb.readEpochLastConfirmedDataComplete(StatusCode.EREADEXCEPTION, null);
            return;
        }
    }

}
