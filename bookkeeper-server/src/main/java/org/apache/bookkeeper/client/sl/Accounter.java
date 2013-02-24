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

import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.AddCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.CloseAccounterCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.OpenCursorCallback;

import com.google.common.annotations.Beta;
import com.google.protobuf.ByteString;

@Beta
public interface Accounter {

    //
    // Data Operations
    //

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
    public void asyncAddEntry(byte[] data, int offset, int length, AddCallback cb, Object ctx);

    //
    // Cursor Operations
    //

    /**
     * Async Open cursor.
     * 
     * @param cursorName
     *            Cursor name.
     * @param callback
     *            Callback object.
     * @param ctx
     *            Callback context.
     */
    public void asyncOpenCursor(ByteString cursorName, OpenCursorCallback callback, Object ctx);

    /**
     * Delete a given cursor by <i>cursorName</i>.
     * 
     * @param cursorName
     *            Cursor Name.
     * @param callback
     *            Callback object.
     * @param ctx
     *            Callback context.
     */
    public void asyncDeleteCursor(ByteString cursorName, DeleteCursorCallback callback, Object ctx);

    /**
     * Close the accounter.
     * 
     * @param callback
     *            Callback object.
     * @param ctx
     *            Callback context.
     */
    public void asyncClose(CloseAccounterCallback callback, Object ctx);

}
