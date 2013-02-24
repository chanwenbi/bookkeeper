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
package org.apache.bookkeeper.util;

import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SuperLedgerRequest;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

public class SuperLedgerUtils {

    public static long EPOCH_MASK = 1L << 32 - 1;

    /**
     * Parse epoch from the entry id.
     * 
     * @param entryId
     *            Entry Id
     * @return epoch
     */
    public static int epoch(long entryId) {
        return (int) (entryId >>> 32);
    }

    /**
     * Generate entry id using <code>epoch</code> and local <code>counter</code>
     * 
     * @param epoch
     *            Epoch number
     * @param counter
     *            Local counter
     * @return entry id
     */
    public static long entryid(int epoch, long counter) {
        return ((long) epoch) << 32 | (counter & EPOCH_MASK);
    }

    public static boolean isValidEntryId(long entryId) {
        long counter = entryId & EPOCH_MASK;
        return !(counter == EPOCH_MASK || counter == 0);
    }

    /**
     * Compute the hash code for <code>ledgerName</code> and
     * <code>acctName</code>.
     * 
     * @param ledgerName
     *            Ledger Name.
     * @param acctName
     *            Acct Name.
     * @return hash code.
     */
    public static int hashCode(ByteString ledgerName, ByteString acctName) {
        return ledgerName.hashCode() * 37 + acctName.hashCode();
    }

    public static String requestToReadbleString(SuperLedgerRequest slr) {
        return TextFormat.printToString(slr);
    }

    public static boolean isNoEntryException(StatusCode code) {
        return StatusCode.ENOSUPERLEDGER == code || StatusCode.ENOACCT == code
                || StatusCode.ENOSLENTRY == code;
    }
}
