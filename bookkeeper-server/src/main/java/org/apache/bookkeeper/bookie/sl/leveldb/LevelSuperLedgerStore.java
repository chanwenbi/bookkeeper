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
package org.apache.bookkeeper.bookie.sl.leveldb;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.sl.SuperLedgerStore;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.AccountMetadata;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SuperLedgerState;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SuperLedgerUtils;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class LevelSuperLedgerStore implements SuperLedgerStore {

    private static final byte SEP = '\001';

    final ServerConfiguration conf;

    // Storage
    final DB metaDB;
    final HashedLevelDB dataDB;
    final ConcurrentMap<ByteString, SuperLedgerState> slCache;

    static class HashedLevelDB {

        private final DB[] dbs;
        private final int num;

        HashedLevelDB(File[] dirs, Options options) throws IOException {
            this.num = dirs.length;
            dbs = new DB[num];
            for (int i = 0; i < num; i++) {
                dbs[i] = factory.open(dirs[i], options);
            }
        }

        DB getDB(ByteString ledgerName, ByteString acctName) {
            if (1 == num) {
                return dbs[0];
            }
            int hashCode = SuperLedgerUtils.hashCode(ledgerName, acctName);
            int idx = MathUtils.signSafeMod(hashCode, num);
            return dbs[idx];
        }

        DB[] getAllDBs() {
            return dbs;
        }

        void close() {
            for (DB db : dbs) {
                db.close();
            }
        }
    }

    public LevelSuperLedgerStore(ServerConfiguration conf) throws IOException {
        this.conf = conf;

        // TODO: separated settings for directories
        File metaDir = conf.getJournalDir();
        File[] dataDirs = conf.getLedgerDirs();

        // super ledger state cache
        slCache = new ConcurrentHashMap<ByteString, SuperLedgerState>();

        metaDB = factory.open(metaDir, getMetaOptions());
        dataDB = new HashedLevelDB(dataDirs, getDataOptions());
    }

    /**
     * Return options used for data db.
     * 
     * @return options for data db.
     */
    private Options getDataOptions() {
        // TODO: pick configuration settings from conf file
        return new Options().blockSize(128 * 1024).cacheSize(128 * 1024 * 1024)
                .compressionType(CompressionType.SNAPPY).writeBufferSize(32 * 1024 * 1024)
                .createIfMissing(true);
    }

    /**
     * Return options used for meta db.
     * 
     * @return options for meta db.
     */
    private Options getMetaOptions() {
        // TODO: pick configuration settings from conf file
        return new Options().blockSize(128 * 1024).cacheSize(256 * 1024 * 1024)
                .compressionType(CompressionType.SNAPPY).writeBufferSize(64 * 1024 * 1024)
                .createIfMissing(true);
    }

    private byte[] getEntryKey(ByteString ledgerName, ByteString acctName, long entryId) {
        int size = ledgerName.size() + acctName.size() + 2 + Long.SIZE;
        byte[] data = new byte[size];
        ByteBuffer buf = ByteBuffer.wrap(data);
        ledgerName.copyTo(buf);
        buf.put(SEP);
        acctName.copyTo(buf);
        buf.put(SEP);
        buf.putLong(entryId);
        return data;
    }

    private byte[] getAcctKey(ByteString ledgerName, ByteString acctName) {
        int size = ledgerName.size() + acctName.size() + 1;
        byte[] data = new byte[size];
        ByteBuffer buf = ByteBuffer.wrap(data);
        ledgerName.copyTo(buf);
        buf.put(SEP);
        acctName.copyTo(buf);
        return data;
    }

    private SuperLedgerState readSuperLedgerState(ByteString ledgerName) throws BookieException, IOException {
        byte[] bytes = null;
        try {
            bytes = metaDB.get(ledgerName.toByteArray());
        } catch (DBException dbe) {
            throw new IOException(dbe);
        }
        if (null == bytes) {
            throw new BookieException.NoSuperLedgerException(ledgerName);
        }
        try {
            return SuperLedgerState.parseFrom(bytes);
        } catch (InvalidProtocolBufferException ipbe) {
            throw new IOException("Failed to parse ledger state for " + ledgerName.toStringUtf8() + " : ",
                    ipbe);
        }
    }

    private void writeSuperLedgerState(ByteString ledgerName, SuperLedgerState state) throws IOException {
        try {
            metaDB.put(ledgerName.toByteArray(), state.toByteArray());
        } catch (DBException dbe) {
            throw new IOException(dbe);
        }
    }

    private SuperLedgerState getSuperLedgerState(ByteString ledgerName, ByteString masterKey)
            throws BookieException, IOException {
        SuperLedgerState state = slCache.get(ledgerName);
        boolean createNewState = false;
        if (state == null) {
            state = readSuperLedgerState(ledgerName);
            if (state == null) {
                if (null == masterKey) {
                    throw new BookieException.NoSuperLedgerException(ledgerName);
                } else {
                    state = SuperLedgerState.newBuilder().setMasterKey(masterKey).build();
                }
            }
            SuperLedgerState oldState = slCache.putIfAbsent(ledgerName, state);
            if (null != oldState) {
                // someone already put the state in the cache.
                state = oldState;
            } else {
                createNewState = true;
            }
        }
        if (createNewState) {
            writeSuperLedgerState(ledgerName, state);
        } else {
            if (!state.getMasterKey().equals(masterKey)) {
                throw new BookieException.BookieUnauthorizedAccessException();
            }
        }
        return state;
    }

    @Override
    public void addEntry(ByteString ledgerName, ByteString acctName, long entryId, ByteString masterKey,
            int epoch, byte[] data) throws IOException, BookieException {
        SuperLedgerState state = getSuperLedgerState(ledgerName, masterKey);
        if (state.getFencedEpoch() <= epoch) {
            throw new BookieException.SuperLedgerFencedException(ledgerName, epoch);
        }
        try {
            dataDB.getDB(ledgerName, acctName).put(getEntryKey(ledgerName, acctName, entryId), data);
        } catch (DBException dbe) {
            throw new IOException(dbe);
        }
    }

    @Override
    public ByteBuffer readEntry(ByteString ledgerName, ByteString acctName, long entryId) throws IOException,
            BookieException {
        getSuperLedgerState(ledgerName, null);
        byte[] data;
        try {
            data = dataDB.getDB(ledgerName, acctName).get(getEntryKey(ledgerName, acctName, entryId));
            if (null == data) {
                throw new BookieException.NoSLEntryException(ledgerName, acctName, entryId);
            }
            return ByteBuffer.wrap(data);
        } catch (DBException dbe) {
            throw new IOException(dbe);
        }
    }

    @Override
    public void fenceLedger(ByteString ledgerName, int epoch, ByteString masterKey) throws IOException,
            BookieException {
        boolean done = false;
        SuperLedgerState newState = null;
        while (!done) {
            SuperLedgerState state = getSuperLedgerState(ledgerName, masterKey);
            if (state.getFencedEpoch() <= epoch) {
                return;
            }
            if (null == newState) {
                newState = SuperLedgerState.newBuilder(state).setFencedEpoch(epoch).build();
            }
            done = slCache.replace(ledgerName, state, newState);
        }
    }

    @Override
    public void deleteLedgerUntilEpoch(ByteString ledgerName, int maxEpoch) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeAccountMetadata(ByteString ledgerName, ByteString acctName, int epoch,
            ByteString masterKey, AccountMetadata metadata) throws IOException, BookieException {
        SuperLedgerState state = getSuperLedgerState(ledgerName, masterKey);
        if (state.getFencedEpoch() <= epoch) {
            throw new BookieException.SuperLedgerFencedException(ledgerName, epoch);
        }
        try {
            metaDB.put(getAcctKey(ledgerName, acctName), metadata.toByteArray());
        } catch (DBException dbe) {
            throw new IOException(dbe);
        }
    }

    @Override
    public AccountMetadata readAccountMetadata(ByteString ledgerName, ByteString acctName)
            throws IOException, BookieException {
        getSuperLedgerState(ledgerName, null);
        byte[] data;
        try {
            data = metaDB.get(getAcctKey(ledgerName, acctName));
            if (null == data) {
                throw new BookieException.NoAccountException(ledgerName, acctName);
            }
            return AccountMetadata.parseFrom(data);
        } catch (DBException dbe) {
            throw new IOException(dbe);
        } catch (InvalidProtocolBufferException ipbe) {
            throw new IOException(ipbe);
        }
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        metaDB.close();
        dataDB.close();
    }

    @Override
    public void registerJMX(BKMBeanInfo parent) {
        // TODO: add jmx for leveldb super ledger store in future.
    }

    @Override
    public void unregisterJMX() {
        // TODO: unregister jmx for leveldb super ledger store in future.
    }

}
