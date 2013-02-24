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

import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.CloseCursorCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.MarkDeletionCallback;
import org.apache.bookkeeper.client.sl.SuperLedgerCallbacks.ReadCallback;

import com.google.common.annotations.Beta;

/**
 * An <i>AccountCursor</i> is a persisted cursor inside a <i>Accounter</i>.
 */
@Beta
public interface AccountCursor {

    /**
     * Get the unique name of the cursor.
     * 
     * @return cursor name.
     */
    public String getName();

    /**
     * Tells whether this cursor has already consumed all the available entries.
     * <p>
     * This method should not be blocking.
     * 
     * @return true if there are pending entries to read, false otherwise.
     */
    public boolean hasMoreEntries();

    /**
     * Asynchronously read entries from the <i>Accounter</i>, up to the
     * specified <i>numEntries</i>. The returned list can be smaller.
     * 
     * @param numEntries
     *            Maximum number of entries to return.
     * @param callback
     *            Callback object.
     * @param ctx
     *            Callback opaque context.
     */
    public void asyncReadEntries(int numEntries, ReadCallback callback, Object ctx);

    /**
     * This indicates that the reader has finished reading all the entries up to
     * <i>lastEntryId</i> (inclusive).
     * 
     * @param lastEntryId
     *            Last entry id that have been successfully read.
     * @param callback
     *            Callback object.
     * @param ctx
     *            Callback opaque context.
     */
    public void asyncMarkDeletion(long lastEntryId, MarkDeletionCallback callback, Object ctx);

    /**
     * Close the cursor and release associated resources.
     * 
     * @param callback
     *            Callback Object.
     * @param ctx
     *            Callback opaque context.
     */
    public void asyncClose(CloseCursorCallback callback, Object ctx);
}
