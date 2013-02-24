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

import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.util.SafeRunnable;

abstract class AccountOp extends SafeRunnable {

    protected final AccounterImpl acct;

    AccountOp(AccounterImpl acct) {
        this.acct = acct;
    }

    boolean isClosedOp() {
        return false;
    }

    /**
     * Fail the operation with the given status <i>code</i>.
     * 
     * @param code
     *            Status Code.
     */
    abstract void failCallback(StatusCode code);

    /**
     * Execute the account operation.
     */
    abstract void executeOp();

    @Override
    public void safeRun() {
        if (acct.initiateAccountOp(this)) {
            executeOp();
        }
    }

}
