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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AccountMetadata;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.CursorMetadata;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.CursorMetadata.Cursor;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.EpochMetadata;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.EpochMetadata.Epoch;

import com.google.protobuf.ByteString;

/**
 * In Memory Account Metadata contains two parts: one is epoch metadata,
 * containing boundaries for each epoch; the other one is cursor metadata,
 * containing positions for each cursor.
 */
public class InMemAccountMetadata {

    final SortedMap<Integer, Long> epochBoundaries;
    final Map<ByteString, Long> cursorStates;
    private long version = 0L;
    private Set<Integer> epochsInRecovery = null;

    InMemAccountMetadata() {
        epochBoundaries = new TreeMap<Integer, Long>();
        cursorStates = new HashMap<ByteString, Long>();
    }

    InMemAccountMetadata initializeEpochBoundaries(EpochMetadata epochMetadata) {
        for (Epoch epoch : epochMetadata.getEpochBoundariesList()) {
            long lastEntryId = epoch.hasLastEntryId() ? epoch.getLastEntryId()
                    : BookieProtocol.INVALID_ENTRY_ID;
            epochBoundaries.put(epoch.getEpoch(), lastEntryId);
        }
        return this;
    }

    InMemAccountMetadata initializeCursorStates(CursorMetadata cursorMetadata) {
        for (Cursor cursor : cursorMetadata.getCursorsList()) {
            long consumedEntryId = cursor.getLastConsumedEntryId();
            cursorStates.put(cursor.getName(), consumedEntryId);
        }
        return this;
    }

    InMemAccountMetadata initializeVersion(long version) {
        this.version = version;
        return this;
    }

    boolean beginRecoverEpoch(int epoch) {
        if (null == epochsInRecovery) {
            epochsInRecovery = new HashSet<Integer>();
        }
        return epochsInRecovery.add(epoch);
    }

    boolean commitRecoverEpoch(int epoch, long lastEntryId) {
        if (null == epochsInRecovery) {
            return false;
        }
        if (epochsInRecovery.remove(epoch)) {
            epochBoundaries.put(epoch, lastEntryId);
            if (epochsInRecovery.isEmpty()) {
                epochsInRecovery = null;
            }
            return true;
        }
        return false;
    }

    boolean rollbackRecoverEpoch(int epoch) {
        if (null == epochsInRecovery) {
            return false;
        }
        if (epochsInRecovery.remove(epoch)) {
            if (epochsInRecovery.isEmpty()) {
                epochsInRecovery = null;
            }
            return true;
        }
        return false;
    }

    synchronized boolean setEpoch(int epoch, long lastEntryId) {
        Long oldEntryId = epochBoundaries.get(epoch);
        if (null == oldEntryId || oldEntryId < lastEntryId) {
            epochBoundaries.put(epoch, lastEntryId);
            return true;
        }
        return false;
    }

    synchronized boolean setCursor(ByteString cursorName, long lastConsumedEntryId) {
        Long oldEntryId = cursorStates.get(cursorName);
        if (null == oldEntryId || oldEntryId < lastConsumedEntryId) {
            cursorStates.put(cursorName, lastConsumedEntryId);
            return true;
        }
        return false;
    }

    synchronized Long getCursor(ByteString cursorName) {
        return cursorStates.get(cursorName);
    }

    synchronized boolean removeCursor(ByteString cursorName) {
        return null != cursorStates.remove(cursorName);
    }

    AccountMetadata.Builder build() {
        AccountMetadata.Builder builder = AccountMetadata.newBuilder();
        EpochMetadata.Builder epochBuilder = EpochMetadata.newBuilder();
        for (Entry<Integer, Long> entry : epochBoundaries.entrySet()) {
            epochBuilder.addEpochBoundaries(Epoch.newBuilder().setEpoch(entry.getKey())
                    .setLastEntryId(entry.getValue()));
        }
        builder.setEpochMetadata(epochBuilder);
        CursorMetadata.Builder cursorBuilder = CursorMetadata.newBuilder();
        for (Entry<ByteString, Long> entry : cursorStates.entrySet()) {
            cursorBuilder.addCursors(Cursor.newBuilder().setName(entry.getKey())
                    .setLastConsumedEntryId(entry.getValue()));
        }
        builder.setCursorMetadata(cursorBuilder);
        builder.setVersion(++version);
        return builder;
    }
}
