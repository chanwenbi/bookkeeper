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
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;

import com.google.protobuf.ByteString;

public class SuperLedgerCallbacks {

    /**
     * Add Entry Callback
     */
    public static interface AddCallback {
        /**
         * Callback Declaration.
         * 
         * @param code
         *            return code.
         * @param acct
         *            accounter handle.
         * @param entryId
         *            entry id.
         * @param ctx
         *            context object.
         */
        public void addComplete(StatusCode code, Accounter acct, long entryId, Object ctx);
    }

    /**
     * Open Cursor Callback.
     */
    public static interface OpenCursorCallback {
        /**
         * Callback Declaration.
         * 
         * @param code
         *            return code.
         * @param cursor
         *            account cursor.
         * @param ctx
         *            context opaque object.
         */
        public void openComplete(StatusCode code, AccountCursor cursor, Object ctx);
    }

    /**
     * Delete Cursor Callback.
     */
    public static interface DeleteCursorCallback {
        /**
         * Callback Declaration.
         * 
         * @param code
         *            return code.
         * @param cursorName
         *            cursor name.
         * @param ctx
         *            context opaque object.
         */
        public void deleteComplete(StatusCode code, ByteString cursorName, Object ctx);
    }

    /**
     * Close Cursor Callback.
     */
    public static interface CloseCursorCallback {
        /**
         * Callback Declaration.
         *
         * @param code
         *            return code.
         * @param ctx
         *            context opaque object.
         */
        public void closeComplete(StatusCode code, Object ctx);
    }

    /**
     * Read Entry Callback
     */
    public static interface ReadCallback {
        /**
         * Callback Declaration.
         * 
         * @param code
         *            return code.
         * @param seq
         *            sequence of entries.
         * @param ctx
         *            context object.
         */
        public void readComplete(StatusCode code, Enumeration<LedgerEntry> seq,
                Object ctx);
    }

    /**
     * Mark deletion callback.
     */
    public static interface MarkDeletionCallback {
        /**
         * Callback Declaration.
         * 
         * @param code
         *            Return code.
         * @param cursor
         *            Account Cursor.
         * @param ctx
         *            Context opaque context.
         */
        public void markComplete(StatusCode code, AccountCursor cursor, Object ctx);
    }

    /**
     * Close Callback.
     */
    public static interface CloseAccounterCallback {
        /**
         * Callback Declaration.
         * 
         * @param code
         *            return code.
         * @param ctx
         *            context object.
         */
        public void closeComplete(StatusCode code, Object ctx);
    }

    /**
     * Open SuperLedger Callback.
     */
    public static interface OpenSuperLedgerCallback {
        /**
         * Callback Declaration.
         * 
         * @param code
         *            return code
         * @param handle
         *            super ledger handle
         * @param ctx
         *            context object.
         */
        public void openComplete(StatusCode code, SuperLedgerHandle handle, Object ctx);
    }

    /**
     * Close SuperLedger Callback.
     */
    public static interface CloseSuperLedgerCallback {
        /**
         * Callback Declaration.
         * 
         * @param code
         *            return code
         * @param handle
         *            super ledger handle
         * @param ctx
         *            context object
         */
        public void closeComplete(StatusCode code, SuperLedgerHandle handle, Object ctx);
    }
}
