/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;

/**
 * Interface for a bookie.
 */
public interface BookieStore {

    /**
     * Add an entry to the bookie store.
     * 
     * @param entry
     *            Entry data.
     * @param cb
     *            Write callback.
     * @param ctx
     *            Write callback context.
     * @param masterKey
     *            Master key
     * @throws IOException
     * @throws BookieException
     */
    public void addEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException;

    /**
     * Recovery add an entry to the bookie store.
     * 
     * @param entry
     *            Entry data.
     * @param cb
     *            Write callback.
     * @param ctx
     *            Write callback context.
     * @param masterKey
     *            Master key.
     * @throws IOException
     * @throws BookieException
     */
    public void recoveryAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException;

    /**
     * Fencing a ledger using given <code>masterKey</code>.
     * 
     * @param ledgerId
     *            Ledger Id.
     * @param masterKey
     *            Master key.
     * @return future object of fencing result.
     * @throws IOException
     * @throws BookieException
     */
    public Future<Boolean> fenceLedger(long ledgerId, byte[] masterKey)
            throws IOException, BookieException;

    /**
     * Read an entry.
     * 
     * @param ledgerId
     *            Ledger Id.
     * @param entryId
     *            Entry Id.
     * @return
     * @throws IOException
     * @throws NoLedgerException
     */
    public ByteBuffer readEntry(long ledgerId, long entryId)
            throws IOException, NoLedgerException;

    /**
     * Delete the given ledger.
     * 
     * @param ledgerId
     *            Ledger Id.
     * @throws IOException
     */
    public void deleteLedger(long ledgerId) throws IOException;

    /**
     * Start the bookie store.
     */
    public void start();

    /**
     * Shutdown the bookie store.
     */
    public void shutdown();

    /**
     * Register JMX.
     * 
     * @param parent
     *            Parent JMX.
     */
    public void registerJMX(BKMBeanInfo parent);

    /**
     * Unregister JMX.
     */
    public void unregisterJMX();

}
