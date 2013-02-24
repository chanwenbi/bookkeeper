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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.DigestManager;
import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.MacDigestManager;
import org.apache.bookkeeper.client.RoundRobinDistributionSchedule;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.CloseAccounterCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.CloseSuperLedgerCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.ReconfigureCallback;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.SuperLedgerManager;
import org.apache.bookkeeper.proto.sl.SuperLedgerClient;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ByteString;

public class SuperLedgerHandle implements ReconfigureCallback {

    final static Logger LOG = LoggerFactory.getLogger(SuperLedgerHandle.class);

    final BookKeeper bk;
    final ScheduledExecutorService scheduler;
    final SuperLedgerClient slClient;

    final ByteString ledgerKey;
    final ByteString ledgerName;
    final DigestManager macManager;
    final DistributionSchedule distributionSchedule;
    final RateLimiter throttler;
    final SuperLedgerMetadata metadata;
    final SuperLedgerManager ledgerManager;

    final ConcurrentMap<ByteString, AccounterImpl> accounters = new ConcurrentHashMap<ByteString, AccounterImpl>();

    volatile int epoch;
    volatile boolean closed = false;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

    SuperLedgerHandle(BookKeeper bk, SuperLedgerManager ledgerManager, ByteString ledgerName,
            SuperLedgerMetadata metadata, ScheduledExecutorService scheduler) throws GeneralSecurityException {
        ClientConfiguration conf = bk.getConf();

        this.bk = bk;
        this.scheduler = scheduler;
        this.ledgerManager = ledgerManager;
        this.slClient = bk.getSuperLedgerClient();
        this.ledgerName = ledgerName;
        this.throttler = RateLimiter.create(conf.getThrottleValue());
        this.metadata = metadata;

        byte[] passwd = metadata.getPassword().toByteArray();
        this.macManager = DigestManager.instantiate(ledgerName.hashCode(), passwd, metadata.getDigestType());
        this.ledgerKey = ByteString.copyFrom(MacDigestManager.genDigest("superledger", passwd));
        this.distributionSchedule = new RoundRobinDistributionSchedule(metadata.getWriteQuorumSize(),
                metadata.getAckQuorumSize(), metadata.getEnsembleSize());
    }

    SuperLedgerHandle setEpoch(final int epoch) {
        this.epoch = epoch;
        // after update the epoch, schedule next reconfiguration
        scheduler.schedule(new Runnable() {
            final int curEpoch = epoch;
            @Override
            public void run() {
                if (curEpoch != SuperLedgerHandle.this.epoch) {
                    // some one already bump the epoch.
                    return;
                }
                reconfigure(curEpoch);
            }

        }, metadata.getEpochTime(), TimeUnit.SECONDS);
        return this;
    }

    /**
     * Get ledger name.
     * 
     * @return ledger name.
     */
    public ByteString getLedgerName() {
        return ledgerName;
    }

    /**
     * Get the ledger metadata.
     * 
     * @return ledger metadata
     */
    SuperLedgerMetadata getLedgerMetadata() {
        return metadata;
    }

    /**
     * Get the digest manager.
     * 
     * @return digest manager for the ledger handle.
     */
    DigestManager getDigestManager() {
        return macManager;
    }

    /**
     * Get the distribution schedule
     * 
     * @return distribution schedule for the ledger handle.
     */
    DistributionSchedule getDistributionSchedule() {
        return distributionSchedule;
    }

    /**
     * Get the accounter instance for given <code>acctName</code>.
     * 
     * @param acctName
     *            Accounter Name.
     * @return accounter instance.
     */
    public Accounter getAccounter(ByteString acctName) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                // if super ledger is closed, return a closed accounter impl
                // so no operation would be executed to this accounter.
                return new AccounterImpl(this, acctName).closeState();
            }
            AccounterImpl acct = accounters.get(acctName);
            if (null == acct) {
                acct = new AccounterImpl(this, acctName);
                AccounterImpl oldAcct = accounters.putIfAbsent(acctName, acct);
                if (null != oldAcct) {
                    acct = oldAcct;
                }
            }
            return acct;
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /**
     * Close accounter instance.
     * 
     * @param acctName
     *            Accounter name.
     * @param callback
     *            Callback instance.
     * @param ctx
     *            Callback context.
     */
    public void closeAccounter(ByteString acctName, CloseAccounterCallback callback,
            Object ctx) {
        closeLock.readLock().lock();
        try {
            Accounter acct = accounters.remove(acctName);
            if (null == acct) {
                return;
            }
            acct.asyncClose(callback, ctx);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    void handleUnrecoverableError() {
        LOG.error("Mark super ledger {} as closed when encountering unrecoverable errors.",
                ledgerName.toStringUtf8());
        closeLock.writeLock().lock();
        try {
            unsafeCloseForUnrecoverableErrors();
        } finally {
            closeLock.writeLock().unlock();
        }
    }

    /**
     * Close super ledger when encountering unrecoverable errors.
     */
    private void unsafeCloseForUnrecoverableErrors() {
        this.unsafeClose(new CloseSuperLedgerCallback() {

            @Override
            public void closeComplete(StatusCode code, SuperLedgerHandle handle, Object ctx) {
                LOG.warn("Closed super ledger {} : {}.", ledgerName.toStringUtf8(), code);
            }

        }, null);
    }

    void reconfigure(int epoch) {
        reconfigure(epoch, this, null);
    }

    /**
     * Reconfigure the super ledger. For now, it just bump the epoch. During the
     * reconfiguration, all outgoing request might received fenced error.
     * 
     * @param epoch
     * @param callback
     * @param ctx
     */
    private void reconfigure(int epoch, ReconfigureCallback callback, Object ctx) {
        StatusCode errCode = null;
        closeLock.writeLock().lock();
        try {
            if (closed) {
                errCode = StatusCode.ESUPERLEDGERCLOSED;
            } else {
                if (epoch < this.epoch) {
                    errCode = StatusCode.EFENCED;
                } else if (epoch == this.epoch) {
                    // bump the epoch first to avoid reconfigure twice.
                    ++this.epoch;
                    new SuperLedgerReconfigureOp(bk, this, callback, ctx).reconfigure();
                } else {
                    // close super ledger when encountering unrecoverable
                    // errors.
                    this.unsafeCloseForUnrecoverableErrors();
                    errCode = StatusCode.ESUPERLEDGERCLOSED;
                }
            }
        } finally {
            closeLock.writeLock().unlock();
        }
        if (null != errCode) {
            callback.reconfigureComplete(errCode, this, ctx);
            return;
        }
    }

    @Override
    public void reconfigureComplete(StatusCode code, SuperLedgerHandle ledgerHandle, Object ctx) {
        if (StatusCode.EOK != code) {
            return;
        }
        closeLock.writeLock().lock();
        try {
            for (AccounterImpl acct : accounters.values()) {
                acct.reconfigure();
            }
        } finally {
            closeLock.writeLock().unlock();
        }
    }

    /**
     * Close super ledger handle.
     * 
     * @param callback
     *            Callback instance.
     * @param context
     *            Callback context.
     */
    public void asyncClose(final CloseSuperLedgerCallback callback, final Object context) {
        closeLock.writeLock().lock();
        try {
            unsafeClose(callback, context);
        } finally {
            closeLock.writeLock().unlock();
        }
    }

    /**
     * Unsafe close super ledger handle.
     * 
     * @param callback
     *            Callback instance.
     * @param context
     *            Callback context.
     */
    private void unsafeClose(final CloseSuperLedgerCallback callback, final Object context) {
        if (closed) {
            callback.closeComplete(StatusCode.ESUPERLEDGERCLOSED, this, context);
            return;
        }
        closed = true;
        if (accounters.isEmpty()) {
            callback.closeComplete(StatusCode.EOK, this, context);
            return;
        }
        final AtomicInteger numAccts = new AtomicInteger(accounters.size());
        for (Entry<ByteString, AccounterImpl> entry : accounters.entrySet()) {
            AccounterImpl acct = entry.getValue();
            acct.asyncClose(new CloseAccounterCallback() {

                @Override
                public void closeComplete(StatusCode code, Object ctx) {
                    if (0 == numAccts.decrementAndGet()) {
                        callback.closeComplete(StatusCode.EOK, SuperLedgerHandle.this, context);
                        return;
                    }
                }

            }, null);
        }
        accounters.clear();
    }
}
