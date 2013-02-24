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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.AddCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.CloseAccounterCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.ReadMetadataCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.WriteMetadataCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AccountMetadata;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.util.SuperLedgerUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Accounter Instance.
 */
class AccounterImpl extends SafeRunnable implements Accounter, ReadMetadataCallback, WriteMetadataCallback {

    static final Logger LOG = LoggerFactory.getLogger(AccounterImpl.class);

    private static final int MAX_METADATA_RETRIES = 5;

    private class DefaultWriteMetadataCallback implements WriteMetadataCallback {

        @Override
        public void writeComplete(StatusCode code, Accounter acct, Object ctx) {
            if (StatusCode.EOK == code) {
                onSuccess(code, acct);
            } else if (StatusCode.EUA == code || StatusCode.EFENCED == code) {
                lh.handleUnrecoverableError();
            } else {
                onFailure(code, acct);
            }
        }

        protected void onSuccess(StatusCode code, Accounter acct) {
            LOG.info("Finished write metadata for accounter {}.", acct);
        }

        protected void onFailure(StatusCode code, Accounter acct) {
            LOG.error("Failed to write metadata for accounter {} : {}", acct, code);
        }

    };

    final SuperLedgerHandle lh;
    final ByteString acctName;
    final int orderKey;

    enum AccountState {
        UNINITIALIZED, INITIALIZING, INITIALIZED, RECONFIGURING, CLOSED
    }

    // Local counter for current epoch.
    long lastAddPushed;
    // last add confirmed entry id.
    long lastAddConfirmed;
    // block add operations
    volatile AccountState state;
    int readMetadataRetries = 0;
    // Epoch Boundaries.
    InMemAccountMetadata metadata = null;

    // Queue of add operations have been sent but wait for responses.
    final LinkedList<AccountAddOp> pendingAddOps = new LinkedList<AccountAddOp>();
    // Queue of account operations haven't been sent.
    LinkedList<AccountOp> blockingAcctOps = null;
    // Queue of read operations havne't been sent.
    Map<Integer, LinkedList<AccountOp>> blockingReadOps = null;

    AccounterImpl(SuperLedgerHandle lh, ByteString acctName) {
        this.lh = lh;
        this.acctName = acctName;
        this.orderKey = SuperLedgerUtils.hashCode(lh.ledgerName, acctName);
        // blocked the add requests until we figured out the lastAddConfirmed.
        state = AccountState.UNINITIALIZED;
    }

    /**
     * @return account name.
     */
    public String getName() {
        StringBuilder sb = new StringBuilder();
        sb.append("(ledger:").append(lh.ledgerName.toStringUtf8()).append(", acct:")
                .append(acctName.toStringUtf8());
        return sb.toString();
    }

    @Override
    public String toString() {
        return getName();
    }

    //
    // Account State
    //

    private void resetAddState(long lastAddConfirmed) {
        lastAddConfirmed = lastAddPushed = lastAddConfirmed;
    }

    private void resetAcctState() {
        state = AccountState.INITIALIZED;
        this.submitBlockingAcctOps();
    }

    /**
     * Put add operation in blocking queue.
     * 
     * @param op
     *            add operation.
     */
    private void blockAccountOp(AccountOp op) {
        if (null == blockingAcctOps) {
            blockingAcctOps = new LinkedList<AccountOp>();
        }
        blockingAcctOps.add(op);
    }

    /**
     * Initiate the account operation.
     * 
     * @param op
     *            account operation
     * @return true if the account op is initiated to execute; otherwise false.
     */
    boolean initiateAccountOp(AccountOp op) {
        if (lh.closed && !op.isClosedOp()) {
            op.failCallback(StatusCode.ESUPERLEDGERCLOSED);
            return false;
        }
        if (AccountState.INITIALIZED == state) {
            return true;
        } else if (AccountState.CLOSED == state) {
            if (op.isClosedOp()) {
                return true;
            } else {
                op.failCallback(StatusCode.EACCTCLOSED);
                return false;
            }
        } else {
            blockAccountOp(op);
            // trigger a readEpoch operation to initialize the add state
            if (AccountState.UNINITIALIZED == state) {
                state = AccountState.INITIALIZING;
                readMetadata();
            }
        }
        return false;
    }

    /**
     * Submit the account op.
     * 
     * @param op
     *            Account Operation.
     */
    void submitAccountOp(AccountOp op) {
        lh.throttler.acquire();
        try {
            lh.bk.getMainWorkerPool().submitOrdered(orderKey, op);
        } catch (RuntimeException re) {
            op.failCallback(StatusCode.EINTR);
        }
    }

    //
    // Add Entry Operations
    //

    /**
     * Initiate adding operation.
     * 
     * @param op
     *            Add operation.
     * @param data
     *            Data to add.
     * @param offset
     *            Offset.
     * @param length
     *            Length.
     */
    void initiateAddOp(AccountAddOp op, byte[] data, int offset, int length) {
        if (op.recoverAdd) {
            doInitiateRecoverAddOp(op, data, offset, length);
        } else {
            doInitiateAddOp(op, data, offset, length);
        }
    }

    private void doInitiateRecoverAddOp(AccountAddOp op, byte[] data, int offset, int length) {
        pendingAddOps.add(op);
        ChannelBuffer toSend = lh.macManager.computeDigestAndPackageForSending(op.entryId, op.entryId - 1, 0,
                data, offset, length);
        ByteBuffer buf = toSend.toByteBuffer();
        op.initiate(op.entryId, buf);
    }

    private void doInitiateAddOp(AccountAddOp op, byte[] data, int offset, int length) {
        ++lastAddPushed;
        pendingAddOps.add(op);
        ChannelBuffer toSend = lh.macManager.computeDigestAndPackageForSending(lastAddPushed,
                lastAddConfirmed, 0, data, offset, length);
        ByteBuffer buf = toSend.toByteBuffer();
        op.initiate(lastAddPushed, buf);
        if (!SuperLedgerUtils.isValidEntryId(lastAddPushed)) {
            state = AccountState.INITIALIZING;
            // we wrap up the counter
            lh.reconfigure(SuperLedgerUtils.epoch(lastAddPushed));
        }
    }

    /**
     * Async add entry to the given account.
     * 
     * @param data
     *            Data bytes.
     * @param offset
     *            Offset
     * @param length
     *            Length
     * @param cb
     *            Add callback
     * @param ctx
     *            Callback context
     */
    @Override
    public void asyncAddEntry(byte[] data, int offset, int length, AddCallback cb, Object ctx) {
        if (offset < 0 || length < 0 || (offset + length) > data.length) {
            throw new ArrayIndexOutOfBoundsException("Invalid values for offset(" + offset + ") or length("
                    + length + ")");
        }
        
        submitAccountOp(new AccountAddOp(this, data, offset, length, cb, ctx));
    }

    //
    // Read Entry Operations
    //
    void blockReadOp(int epoch, AccountOp op) {
        if (null == this.blockingReadOps) {
            this.blockingReadOps = new HashMap<Integer, LinkedList<AccountOp>>();
        }
        LinkedList<AccountOp> epochOps = this.blockingReadOps.get(epoch);
        if (null == epochOps) {
            epochOps = new LinkedList<AccountOp>();
            this.blockingReadOps.put(epoch, epochOps);
        }
        epochOps.add(op);
    }

    void submitBlockedReadOps(int epoch) {
        LinkedList<AccountOp> epochOps = this.blockingReadOps.remove(epoch);
        if (null == epochOps) {
            return;
        }
        for (AccountOp op : epochOps) {
            submitAccountOp(op);
        }
        if (this.blockingReadOps.isEmpty()) {
            this.blockingReadOps = null;
        }
    }

    void submitBlockedReadOps() {
        for (LinkedList<AccountOp> epochOps : this.blockingReadOps.values()) {
            for (AccountOp op : epochOps) {
                submitAccountOp(op);
            }
        }
        this.blockingReadOps.clear();
        this.blockingReadOps = null;
    }

    //
    // Cursor Operations.
    //

    @Override
    public void asyncOpenCursor(ByteString cursorName, OpenCursorCallback callback, Object ctx) {
        submitAccountOp(new CursorOpenOp(this, cursorName, callback, ctx));
    }

    @Override
    public void asyncDeleteCursor(ByteString cursorName, DeleteCursorCallback callback, Object ctx) {
        submitAccountOp(new CursorDeleteOp(this, cursorName, callback, ctx));
    }

    /**
     * Mark state as closed.
     */
    AccounterImpl closeState() {
        state = AccountState.CLOSED;
        return this;
    }

    @Override
    public void asyncClose(final CloseAccounterCallback callback, final Object context) {
        submitAccountOp(new AccountOp(this) {

            @Override
            boolean isClosedOp() {
                return true;
            }

            @Override
            void failCallback(StatusCode code) {
                callback.closeComplete(code, context);
            }

            @Override
            void executeOp() {
                internalClose();
                writeAcctMetadataOnClose(callback, context);
            }

        });
    }

    SuperLedgerHandle getSuperLedgerHandle() {
        return lh;
    }

    DistributionSchedule getDistributionSchedule() {
        return lh.getDistributionSchedule();
    }

    /**
     * Pop all succeed adding operations from pending add queue. If there are
     * fenced add operations, blocked incoming add requests, fenced all pending
     * add operations, updated acct metadata. After acct metadata is updated,
     * clear the state and resubmit all pending add operations and blocking add
     * operations in order.
     */
    void sendAddSuccessCallbacks() {
        // Start from the head of the queue and proceed while there are
        // entries that have had all their responses come back
        AccountAddOp pendingAddOp;
        boolean fenced = false;
        while ((pendingAddOp = pendingAddOps.peek()) != null) {
            if (AccountAddOp.State.PENDING == pendingAddOp.state) {
                return;
            } else if (AccountAddOp.State.FENCED == pendingAddOp.state) {
                if (AccountState.INITIALIZED == state) {
                    state = AccountState.INITIALIZING;
                }
                fenced = true;
                break;
            }
            pendingAddOps.remove();
            lastAddConfirmed = pendingAddOp.entryId;
            pendingAddOp.submitCallback(StatusCode.EOK);
        }
        if (fenced) {
            fenceAllPendingAddOps();
        }
        // check if reconfigured required
        reconfigureAcctIfNecessary();
    }

    /**
     * Fencing all the pending add operations.
     */
    private void fenceAllPendingAddOps() {
        int size = pendingAddOps.size();
        for (int i = size - 1; i >= 0; i--) {
            AccountAddOp pendingAddOp = pendingAddOps.get(i);
            pendingAddOp.fence();
            blockingAcctOps.addFirst(pendingAddOp);
        }
        pendingAddOps.clear();
    }

    private void submitBlockingAcctOps() {
        if (null != blockingAcctOps) {
            for (AccountOp pendingOp : blockingAcctOps) {
                submitAccountOp(pendingOp);
            }
            blockingAcctOps = null;
        }
    }

    private void failAllPendingOps(StatusCode code) {
        for (AccountAddOp addOp : pendingAddOps) {
            addOp.failCallback(code);
        }
        pendingAddOps.clear();
    }

    private void failAllBlockingOps(StatusCode code) {
        for (AccountOp pendingOp : blockingAcctOps) {
            pendingOp.failCallback(code);
        }
        blockingAcctOps = null;
    }

    private void internalClose() {
        state = AccountState.CLOSED;
        failAllPendingOps(StatusCode.EACCTCLOSED);
        failAllBlockingOps(StatusCode.EACCTCLOSED);
    }

    //
    // Reconfiguration to bump Epoch
    //

    /**
     * Reconfigure the accounter. If there is no add operations pending in the
     * queue, write acct metadata in asynchronous way, clear state and submit
     * blocking add operations in queue. If there is pending add operations in
     * queue, submit possible success callbacks in pending queue.
     */
    void reconfigure() {
        lh.bk.getMainWorkerPool().submitOrdered(orderKey, this);
    }

    private void reconfigureAcctIfNecessary() {
        if (pendingAddOps.isEmpty() && AccountState.RECONFIGURING == state
                && SuperLedgerUtils.epoch(lastAddPushed) < lh.epoch) {
            // no operations is pending, close current epoch
            final int newEpoch = lh.epoch;
            writeReconfiguredAcctMetadata(newEpoch, new DefaultWriteMetadataCallback() {

                @Override
                protected void onSuccess(StatusCode code, Accounter acct) {
                    super.onSuccess(code, acct);
                    resetAddState(SuperLedgerUtils.entryid(newEpoch, 0));
                    resetAcctState();
                }

                @Override
                protected void onFailure(StatusCode code, Accounter acct) {
                    super.onFailure(code, acct);
                    internalClose();
                }

            });
        }
    }

    @Override
    public void safeRun() {
        boolean closed = lh.closed || AccountState.CLOSED == state;
        if (!closed) {
            state = AccountState.RECONFIGURING;
            sendAddSuccessCallbacks();
        } else {
            internalClose();
        }
    }

    //
    // Metadata Operations
    //

    private void writeReconfiguredAcctMetadata(final int newEpoch, WriteMetadataCallback cb) {
        boolean writeMetadata = metadata.setEpoch(SuperLedgerUtils.epoch(lastAddConfirmed), lastAddConfirmed);
        writeMetadata |= metadata.setEpoch(newEpoch, BookieProtocol.INVALID_ENTRY_ID);
        if (!writeMetadata) {
            cb.writeComplete(StatusCode.EOK, this, null);
            return;
        }
        doWriteAcctMetadata(cb);
    }

    private void writeAcctMetadata(WriteMetadataCallback cb) {
        boolean writeMetadata = metadata.setEpoch(SuperLedgerUtils.epoch(lastAddPushed),
                BookieProtocol.INVALID_ENTRY_ID);
        if (!writeMetadata) {
            cb.writeComplete(StatusCode.EOK, this, null);
            return;
        }
        doWriteAcctMetadata(cb);
    }

    private void writeAcctMetadataOnClose(final CloseAccounterCallback callback, final Object ctx) {
        boolean writeMetadata = metadata.setEpoch(SuperLedgerUtils.epoch(lastAddConfirmed), lastAddConfirmed);
        if (!writeMetadata) {
            callback.closeComplete(StatusCode.EOK, ctx);
            return;
        }
        AccountMetadata.Builder metadataBuilder = metadata.build();
        submitAccountOp(new AccountMetadataWriteOp(this, metadataBuilder, new WriteMetadataCallback() {

            @Override
            public void writeComplete(StatusCode code, Accounter acct, Object ctx) {
                callback.closeComplete(code, ctx);
            }

        }, ctx) {
            @Override
            boolean isClosedOp() {
                return true;
            }
        });
    }

    void doWriteAcctMetadata(WriteMetadataCallback cb) {
        AccountMetadata.Builder metadataBuilder = metadata.build();
        submitAccountOp(new AccountMetadataWriteOp(this, metadataBuilder, cb, null));
    }

    @Override
    public void writeComplete(StatusCode code, Accounter acct, Object ctx) {
        if (StatusCode.EOK == code) {
            resetAcctState();
        } else if (StatusCode.EUA == code || StatusCode.EFENCED == code) {
            lh.handleUnrecoverableError();
        } else {
            LOG.error("Failed to write account metadata for {}, closed the accounter : {}", this, code);
            internalClose();
        }
    }

    private void readMetadata() {
        submitAccountOp(new AccountMetadataReadOp(this, this, null));
    }

    @Override
    public void readComplete(StatusCode code, Accounter acct, AccountMetadata acctMetadata, long lastEntryId,
            Object ctx) {
        if (StatusCode.EUA == code) {
            lh.handleUnrecoverableError();
            return;
        }

        if (StatusCode.EOK == code || SuperLedgerUtils.isNoEntryException(code)) {
            if (AccountState.INITIALIZED != state) {
                if (null == metadata) {
                    metadata = new InMemAccountMetadata();
                    if (null != acctMetadata) {
                        metadata.initializeVersion(acctMetadata.getVersion());
                        if (acctMetadata.hasEpochMetadata()) {
                            metadata.initializeEpochBoundaries(acctMetadata.getEpochMetadata());
                            for (Entry<Integer, Long> boundary : metadata.epochBoundaries.entrySet()) {
                                // recovery those unclosed epochs.
                                if (BookieProtocol.INVALID_ENTRY_ID == boundary.getValue()) {
                                    this.submitAccountOp(new EpochRecoveryOp(this, boundary.getKey()));
                                }
                            }
                        }
                        if (acctMetadata.hasCursorMetadata()) {
                            metadata.initializeCursorStates(acctMetadata.getCursorMetadata());
                        }
                    }
                    // valid response for last entry id.
                    if (BookieProtocol.INVALID_ENTRY_ID == lastEntryId
                            || SuperLedgerUtils.epoch(lastEntryId) < lh.epoch) {
                        // no entries found, first time to add entries at this
                        // epoch.
                        resetAddState(SuperLedgerUtils.entryid(lh.epoch, 0));
                        writeAcctMetadata(this);
                    } else {
                        // we are at same epoch with last entry id.
                        // it means that the accounter is opened again during
                        // this epoch. we could use the last entry id directly.
                        resetAddState(lastEntryId);
                        resetAcctState();
                    }
                }
            }
        } else {
            if (++readMetadataRetries < MAX_METADATA_RETRIES) {
                LOG.info("Failed to read accounter metadata for {}, retrying ...", getName());
                readMetadata();
                return;
            } else {
                LOG.error("Failed to read accounter metadata for {} after {} retries.", new Object[] {
                        getName(), readMetadataRetries });
                internalClose();
                return;
            }
        }

    }

}
