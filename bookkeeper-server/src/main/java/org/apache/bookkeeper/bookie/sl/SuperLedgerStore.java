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
package org.apache.bookkeeper.bookie.sl;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AccountMetadata;

import com.google.protobuf.ByteString;

/**
 * Interface for a superledger store.
 */
public interface SuperLedgerStore {

    /**
     * Add an entry for given account in the super ledger.
     * 
     * @param ledgerName
     *            Ledger Name.
     * @param acctName
     *            Account Name.
     * @param entryId
     *            Entry Id.
     * @param masterKey
     *            Master Key.
     * @param epoch
     *            Epoch.
     * @param data
     *            Data to add.
     * @throws IOException
     * @throws BookieException
     */
    public void addEntry(ByteString ledgerName, ByteString acctName, long entryId, ByteString masterKey,
            int epoch, byte[] data) throws IOException, BookieException;

    /**
     * Read an entry from given account in the super ledger.
     * 
     * @param ledgerName
     *            Ledger Name
     * @param acctName
     *            Account Name
     * @param entryId
     *            Entry Id
     * @return entry data.
     * @throws IOException
     * @throws BookieException
     */
    public ByteBuffer readEntry(ByteString ledgerName, ByteString acctName, long entryId) throws IOException,
            BookieException;

    /**
     * Fencing a ledger at given <i>epoch</i> using given <i>masterKey</i>.
     * 
     * @param ledgerName
     *            Ledger Name.
     * @param epoch
     *            Epoch to fence.
     * @param masterKey
     *            Master Key.
     * @throws IOException
     * @throws BookieException
     */
    public void fenceLedger(ByteString ledgerName, int epoch, ByteString masterKey) throws IOException,
            BookieException;
    
    /**
     * Delete the entries of given ledger until <code>maxEpoch</code>.
     * 
     * @param ledgerName
     *            Ledger Name.
     * @param maxEpoch
     *            Max Epoch.
     * @throws IOException
     */
    public void deleteLedgerUntilEpoch(ByteString ledgerName, int maxEpoch) throws IOException;

    /**
     * Write metadata to given account.
     * 
     * @param ledgerName
     *            Ledger Name.
     * @param acctName
     *            Account Name.
     * @param epoch
     *            Epoch.
     * @param masterKey
     *            Master key.
     * @param metadata
     *            Account Metadata.
     * @throws IOException
     * @throws BookieException
     */
    public void writeAccountMetadata(ByteString ledgerName, ByteString acctName, int epoch,
            ByteString masterKey, AccountMetadata metadata) throws IOException, BookieException;

    /**
     * Read metadata of given account.
     * 
     * @param ledgerName
     *            Ledger Name.
     * @param acctName
     *            Account Name.
     * @return account metadata.
     * @throws IOException
     * @throws BookieException
     */
    public AccountMetadata readAccountMetadata(ByteString ledgerName, ByteString acctName)
            throws IOException, BookieException;

    /**
     * Start the superledger store.
     */
    public void start();

    /**
     * Shtudown the superledger store.
     */
    public void stop();

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
