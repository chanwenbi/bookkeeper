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

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.FenceSuperLedgerCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.ReconfigureCallback;
import org.apache.bookkeeper.meta.SuperLedgerManager.SuperLedgerMetadataCallback;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SuperLedgerReconfigureOp implements SuperLedgerMetadataCallback<Void>,
        FenceSuperLedgerCallback {

    static final Logger LOG = LoggerFactory.getLogger(SuperLedgerReconfigureOp.class);

    final BookKeeper bk;
    final SuperLedgerHandle lh;
    final ReconfigureCallback cb;
    final Object ctx;

    SuperLedgerReconfigureOp(BookKeeper bk, SuperLedgerHandle lh, ReconfigureCallback cb, Object ctx) {
        this.bk = bk;
        this.lh = lh;
        this.cb = cb;
        this.ctx = ctx;
    }

    void reconfigure() {
        bk.getMainWorkerPool().submitOrdered(lh.ledgerName.hashCode(),
                new SuperLedgerFenceOp(lh, lh.metadata.getEpoch(), this, null));
    }

    @Override
    public void complete(StatusCode code, Void value) {
        if (StatusCode.EOK == code) {
            cb.reconfigureComplete(code, lh.setEpoch(lh.metadata.getEpoch()), ctx);
        } else if (StatusCode.EBADMETADATAVERSION == code) {
            // TODO: for now we just fail it.
            LOG.error("Failed to write super ledger metadata for {} : {}", lh.ledgerName.toStringUtf8(), code);
            failCallback(code);
        } else {
            LOG.error("Failed to write super ledger metadata for {} : {}", lh.ledgerName.toStringUtf8(), code);
            failCallback(code);
        }
    }

    @Override
    public void fenceComplete(StatusCode code, SuperLedgerHandle ledgerHandle, Object ctx) {
        if (StatusCode.EOK == code) {
            lh.ledgerManager.writeSuperLedgerMetadata(lh.ledgerName, lh.metadata.bumpEpoch(), this);
            return;
        } else {
            failCallback(code);
            return;
        }
    }

    private void failCallback(StatusCode code) {
        lh.handleUnrecoverableError();
        // if failed to reconfigure
        cb.reconfigureComplete(code, lh, this.ctx);
    }

}
