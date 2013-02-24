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

import java.security.GeneralSecurityException;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.OpenSuperLedgerCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.FenceSuperLedgerCallback;
import org.apache.bookkeeper.meta.SuperLedgerManager;
import org.apache.bookkeeper.meta.SuperLedgerManager.SuperLedgerMetadataCallback;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.versioning.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class SuperLedgerOpenOp implements SuperLedgerMetadataCallback<SuperLedgerMetadata>,
        FenceSuperLedgerCallback {

    static final Logger LOG = LoggerFactory.getLogger(SuperLedgerOpenOp.class);

    final BookKeeper bk;
    final ScheduledExecutorService scheduler;
    final SuperLedgerManager ledgerManager;
    final ByteString ledgerName;
    final SuperLedgerConfig config;
    final OpenSuperLedgerCallback cb;
    final Object ctx;

    public SuperLedgerOpenOp(BookKeeper bk, ScheduledExecutorService scheduler,
            SuperLedgerManager ledgerManager, ByteString ledgerName, SuperLedgerConfig config,
            OpenSuperLedgerCallback cb, Object ctx) {
        this.bk = bk;
        this.scheduler = scheduler;
        this.ledgerManager = ledgerManager;
        this.ledgerName = ledgerName;
        this.config = config;
        this.cb = cb;
        this.ctx = ctx;
    }

    public void initiate() {
        ledgerManager.readSuperLedgerMetadata(ledgerName, this);
    }

    @Override
    public void complete(StatusCode code, SuperLedgerMetadata metadata) {
        if (StatusCode.ENOSUPERLEDGER == code) {
            if (config.createIfMissing()) {
                SuperLedgerMetadata newMetadata = config.toMetadata();
                newMetadata.setVersion(Version.NEW);
                SuperLedgerHandle slh = newSuperLedgerHandle(newMetadata);
                if (null != slh) {
                    writeSuperLedgerMetadata(slh, newMetadata);
                    return;
                }
            } else {
                cb.openComplete(StatusCode.ENOSUPERLEDGER, null, ctx);
            }
            return;
        }
        if (StatusCode.EOK != code) {
            // open super ledger failed.
            cb.openComplete(code, null, ctx);
            return;
        }
        if (!config.password().equals(metadata.getPassword())) {
            LOG.error("Provided passwd does not match that in metadata for super ledger {}.", ledgerName.toStringUtf8());
            cb.openComplete(StatusCode.EUA, null, ctx);
            return;
        }
        if (config.digestType() != metadata.getDigestType()) {
            LOG.error("Provided digest type does not match that in metadata for super ledger {}.",
                    ledgerName.toStringUtf8());
            cb.openComplete(StatusCode.EBADDIGEST, null, ctx);
        }
        SuperLedgerHandle slh = newSuperLedgerHandle(metadata);
        if (null != slh) {
            // fence the ledger
            bk.getMainWorkerPool().submitOrdered(ledgerName.hashCode(),
                    new SuperLedgerFenceOp(slh, metadata.getEpoch(), this, ctx));
            return;
        }
    }
    
    private SuperLedgerHandle newSuperLedgerHandle(SuperLedgerMetadata metadata) {
        SuperLedgerHandle slh;
        try {
            slh = new SuperLedgerHandle(bk, ledgerManager, ledgerName, metadata, scheduler);
        } catch (GeneralSecurityException e) {
            LOG.error("Security exception while opening super ledger: " + ledgerName.toStringUtf8(), e);
            cb.openComplete(StatusCode.EBADDIGEST, null, this.ctx);
            return null;
        }
        
        return slh;
    }

    @Override
    public void fenceComplete(StatusCode code, SuperLedgerHandle ledgerHandle, Object ctx) {
        if (StatusCode.EOK == code) {
            writeSuperLedgerMetadata(ledgerHandle, ledgerHandle.metadata);
            return;
        } else {
            cb.openComplete(code, null, this.ctx);
            return;
        }
    }
    
    private void writeSuperLedgerMetadata(final SuperLedgerHandle lh, SuperLedgerMetadata metadata) {
        ledgerManager.writeSuperLedgerMetadata(ledgerName, metadata.bumpEpoch(),
                new SuperLedgerMetadataCallback<Void>() {

                    @Override
                    public void complete(StatusCode code, Void value) {
                        if (StatusCode.EOK == code) {
                            cb.openComplete(code, lh.setEpoch(lh.metadata.getEpoch()),
                                    SuperLedgerOpenOp.this.ctx);
                        } else if (StatusCode.EBADMETADATAVERSION == code) {
                            // TODO: for now we just fail it.
                            LOG.error("Failed to write super ledger metadata for {} : {}",
                                    ledgerName.toStringUtf8(), code);
                            cb.openComplete(code, null, SuperLedgerOpenOp.this.ctx);
                        } else {
                            LOG.error("Failed to write super ledger metadata for {} : {}",
                                    ledgerName.toStringUtf8(), code);
                            cb.openComplete(code, null, SuperLedgerOpenOp.this.ctx);
                        }
                    }

                });
    }

}
