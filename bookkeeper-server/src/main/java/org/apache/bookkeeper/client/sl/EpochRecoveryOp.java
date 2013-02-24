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

import java.util.Enumeration;

import org.apache.bookkeeper.client.DigestManager.RecoveryData;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.sl.EpochReadLastConfirmedOp.EpochLastConfirmedDataCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.AddCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.ReadCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.WriteMetadataCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.util.SuperLedgerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EpochRecoveryOp extends AccountOp implements EpochLastConfirmedDataCallback, WriteMetadataCallback,
        ReadCallback, AddCallback {

    static final Logger LOG = LoggerFactory.getLogger(EpochRecoveryOp.class);
    int numResponsesPending;
    final int epoch;
    long lastAddConfirmed;

    EpochRecoveryOp(AccounterImpl acct, int epoch) {
        super(acct);
        this.epoch = epoch;
        this.numResponsesPending = acct.lh.metadata.getEnsembleSize();
        lastAddConfirmed = SuperLedgerUtils.entryid(epoch, 0);
    }

    void initiate() {
        EpochReadLastConfirmedOp erlcop = new EpochReadLastConfirmedOp(acct, epoch, this);
        acct.submitAccountOp(erlcop);
    }

    private void retry() {
        acct.metadata.rollbackRecoverEpoch(epoch);
        initiate();
    }

    @Override
    public void readEpochLastConfirmedDataComplete(StatusCode code, RecoveryData data) {
        if (StatusCode.EOK == code) {
            lastAddConfirmed = data.lastAddConfirmed;
            doRecoveryRead();
        } else {
            // retry later.
            retry();
        }
    }

    @Override
    public void addComplete(StatusCode code, Accounter acct, long entryId, Object ctx) {
        if (StatusCode.EOK != code) {
            // Give up, we can't recover from this error
            LOG.error("Failed to writing entry {} of acct {} while recovering epoch {} : {}", new Object[] {
                    lastAddConfirmed, acct, epoch, code });
            retry();
        }
        doRecoveryRead();
    }

    private void doRecoveryRead() {
        ++lastAddConfirmed;
        acct.submitAccountOp(new AccountReadOp(acct, acct.lh.scheduler, lastAddConfirmed, lastAddConfirmed,
                this, null));
    }

    @Override
    public void readComplete(StatusCode code, Enumeration<LedgerEntry> seq, Object ctx) {
        if (StatusCode.EOK == code) {
            LedgerEntry entry = seq.nextElement();
            byte[] data = entry.getEntry();
            acct.submitAccountOp(new AccountAddOp(acct, data, 0, data.length, this, ctx)
                    .enableRecoveryAdd(lastAddConfirmed));
            return;
        }
        if (SuperLedgerUtils.isNoEntryException(code)) {
            --lastAddConfirmed;
            boolean committed = acct.metadata.commitRecoverEpoch(epoch, lastAddConfirmed);
            acct.submitBlockedReadOps(epoch);
            if (committed) {
                acct.doWriteAcctMetadata(this);
            }
            return;
        }
        // some other error, we can't handle
        LOG.error("Faile to read entry {} of acct {} while recovering epoch {} : {}", new Object[] {
                lastAddConfirmed, acct, epoch, code });
        retry();
    }

    @Override
    public void writeComplete(StatusCode code, Accounter acct, Object ctx) {
        LOG.info("Finished write epoch metatdata for acct {} after recovered epoch {} : {}", new Object[] {
                acct, epoch, code });
    }

    @Override
    void failCallback(StatusCode code) {
        // do nothing now.
    }

    @Override
    void executeOp() {
        Long boundary = acct.metadata.epochBoundaries.get(epoch);
        if (null == boundary) {
            LOG.error("No epoch {} found for account {}.", epoch, acct);
            return;
        }
        if (boundary != BookieProtocol.INVALID_ENTRY_ID) {
            return;
        }
        if (acct.metadata.beginRecoverEpoch(epoch)) {
            initiate();
        } else {
            LOG.info("Epoch {} for account {} is recovering by others.", epoch, acct);
        }
    }

}
