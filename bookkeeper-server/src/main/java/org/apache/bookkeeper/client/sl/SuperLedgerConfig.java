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

import static com.google.common.base.Charsets.UTF_8;

import org.apache.bookkeeper.client.BookKeeper.DigestType;

import com.google.protobuf.ByteString;

/**
 * Super ledger configuration.
 */
public class SuperLedgerConfig {

    private int ensembleSize;
    private int writeQuorumSize;
    private int ackQuorumSize;

    private DigestType digestType;
    private ByteString password;
    private long epochTime;

    private boolean createIfMissing = false;

    public boolean createIfMissing() {
        return createIfMissing;
    }

    public SuperLedgerConfig createIfMissing(boolean createIfMissing) {
        this.createIfMissing = createIfMissing;
        return this;
    }

    public int ensembleSize() {
        return ensembleSize;
    }

    public SuperLedgerConfig ensmebleSize(int ensembleSize) {
        this.ensembleSize = ensembleSize;
        return this;
    }

    public int writeQuorumSize() {
        return writeQuorumSize;
    }

    public SuperLedgerConfig writeQuorumSize(int writeQuorumSize) {
        this.writeQuorumSize = writeQuorumSize;
        return this;
    }

    public int ackQuorumSize() {
        return ackQuorumSize;
    }

    public SuperLedgerConfig ackQuorumSize(int ackQuorumSize) {
        this.ackQuorumSize = ackQuorumSize;
        return this;
    }

    public DigestType digestType() {
        return digestType;
    }

    public SuperLedgerConfig digestType(DigestType digestType) {
        this.digestType = digestType;
        return this;
    }

    public ByteString password() {
        return password;
    }

    public SuperLedgerConfig password(ByteString passwd) {
        this.password = passwd;
        return this;
    }

    public long epochTime() {
        return epochTime;
    }

    public SuperLedgerConfig epochTime(long epochTime) {
        this.epochTime = epochTime;
        return this;
    }

    public boolean validate() {
        if (ensembleSize <= 0 || writeQuorumSize <= 0 || ackQuorumSize <= 0) {
            return false;
        }
        if (writeQuorumSize > ensembleSize || ackQuorumSize > writeQuorumSize) {
            return false;
        }
        if (null == digestType || null == password || epochTime <= 0) {
            return false;
        }
        return true;
    }

    public SuperLedgerMetadata toMetadata() {
        return new SuperLedgerMetadata(ensembleSize, writeQuorumSize, ackQuorumSize, digestType, password,
                epochTime);
    }

    @Override
    public String toString() {
        return new String(toMetadata().serialize(), UTF_8);
    }
}
