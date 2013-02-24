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

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.CloseCursorCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.MarkDeletionCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.ReadCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.WriteMetadataCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.util.SuperLedgerUtils;

import com.google.protobuf.ByteString;

class AccountCursorImpl implements AccountCursor {

    final AccounterImpl acct;
    final ByteString cursorName;
    long lastConsumedEntryId;

    AccountCursorImpl(AccounterImpl acct, ByteString cursorName, long lastConsumedEntryId) {
        this.acct = acct;
        this.cursorName = cursorName;
        this.lastConsumedEntryId = lastConsumedEntryId;
    }

    @Override
    public String getName() {
        return String.format("(acct:%s, cursor:%s)", acct, cursorName);
    }

    @Override
    public boolean hasMoreEntries() {
        return lastConsumedEntryId < acct.lastAddConfirmed;
    }

    @Override
    public void asyncReadEntries(final int numEntries, final ReadCallback callback, final Object ctx) {
        AccountOp readOp = new AccountOp(acct) {

            @Override
            void failCallback(StatusCode code) {
                callback.readComplete(code, null, ctx);
            }

            @Override
            void executeOp() {
                long startEntryId = lastConsumedEntryId + 1;
                if (SuperLedgerUtils.epoch(startEntryId) == SuperLedgerUtils
                        .epoch(acct.lastAddConfirmed)) {
                    if (startEntryId >= acct.lastAddConfirmed) {
                        callback.readComplete(StatusCode.EOK, new Enumeration<LedgerEntry>() {

                            @Override
                            public boolean hasMoreElements() {
                                return false;
                            }

                            @Override
                            public LedgerEntry nextElement() {
                                return null;
                            }

                        }, ctx);
                    } else {
                        readEntries(startEntryId, numEntries, callback, ctx);
                    }
                } else {
                    int epoch = SuperLedgerUtils.epoch(startEntryId);
                    Long epochEndEntryId = acct.metadata.epochBoundaries.get(epoch);
                    if (null == epochEndEntryId) {
                        epoch = acct.metadata.epochBoundaries.tailMap(epoch + 1).firstKey();
                        epochEndEntryId = acct.metadata.epochBoundaries.get(epoch);
                    }
                    if (BookieProtocol.INVALID_ENTRY_ID == epochEndEntryId) {
                        acct.blockReadOp(epoch, this);
                    } else {
                        if (!SuperLedgerUtils.isValidEntryId(startEntryId)) {
                            ++startEntryId;
                        }
                        readEntries(startEntryId, numEntries, callback, ctx);
                    }
                }
            }

        };
        acct.submitAccountOp(readOp);
    }

    private void readEntries(long startEntryId, int numEntries, final ReadCallback callback, Object ctx) {
        final long endEntryId = Math.min(startEntryId + numEntries - 1, acct.lastAddConfirmed);
        new AccountReadOp(acct, acct.lh.scheduler, startEntryId, endEntryId, new ReadCallback() {

            @Override
            public void readComplete(StatusCode code, Enumeration<LedgerEntry> seq, Object ctx) {
                if (StatusCode.EOK == code) {
                    lastConsumedEntryId = endEntryId;
                }
                callback.readComplete(code, seq, ctx);
            }

        }, ctx);
    }

    @Override
    public void asyncMarkDeletion(long lastEntryId, final MarkDeletionCallback callback, final Object context) {
        this.lastConsumedEntryId = lastEntryId;
        updateCursorState(new WriteMetadataCallback() {

            @Override
            public void writeComplete(StatusCode code, Accounter acct, Object ctx) {
                callback.markComplete(code, AccountCursorImpl.this, context);
            }

        });
    }

    @Override
    public void asyncClose(final CloseCursorCallback callback, final Object context) {
        updateCursorState(new WriteMetadataCallback() {

            @Override
            public void writeComplete(StatusCode code, Accounter acct, Object ctx) {
                callback.closeComplete(code, context);
            }

        });
    }

    private void updateCursorState(WriteMetadataCallback callback) {
        if (!acct.metadata.setCursor(cursorName, lastConsumedEntryId)) {
            callback.writeComplete(StatusCode.EOK, acct, null);
            return;
        }
        acct.doWriteAcctMetadata(callback);
    }

}
