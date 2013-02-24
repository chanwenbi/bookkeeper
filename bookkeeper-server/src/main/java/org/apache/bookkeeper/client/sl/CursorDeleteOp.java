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

import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerInternalCallbacks.WriteMetadataCallback;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;

import com.google.protobuf.ByteString;

class CursorDeleteOp extends AccountOp implements WriteMetadataCallback {

    private final ByteString cursorName;
    private final DeleteCursorCallback callback;
    private final Object ctx;

    CursorDeleteOp(AccounterImpl acct, ByteString cursorName, DeleteCursorCallback callback, Object ctx) {
        super(acct);
        this.cursorName = cursorName;
        this.callback = callback;
        this.ctx = ctx;
    }

    @Override
    void executeOp() {
        if (acct.metadata.removeCursor(cursorName)) {
            acct.doWriteAcctMetadata(this);
        } else {
            callback.deleteComplete(StatusCode.ENOCUSOR, cursorName, ctx);
        }
    }

    void failCallback(StatusCode code) {
        this.callback.deleteComplete(code, cursorName, ctx);
    }

    @Override
    public void writeComplete(StatusCode code, Accounter acct, Object ctx) {
        this.callback.deleteComplete(code, cursorName, ctx);
    }

}
