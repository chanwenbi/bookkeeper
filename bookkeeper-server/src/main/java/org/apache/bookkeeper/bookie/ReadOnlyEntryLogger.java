/*
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

package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.bookkeeper.conf.ServerConfiguration;

/**
 * Read Only Entry Logger
 */
public class ReadOnlyEntryLogger extends EntryLogger {

    public ReadOnlyEntryLogger(ServerConfiguration conf) throws IOException {
        super(conf, new LedgerDirsManager(conf, conf.getLedgerDirs()));
    }

    @Override
    protected void initialize() throws IOException {
        // do nothing for read only entry logger
    }

    @Override
    void createNewLog() throws IOException {
        throw new IOException("Can't create new entry log using a readonly entry logger.");
    }

    @Override
    protected boolean removeEntryLog(long entryLogId) {
        // can't remove entry log in readonly mode
        return false;
    }

    @Override
    synchronized long addEntry(long ledger, ByteBuffer entry) throws IOException {
        throw new IOException("Can't add entry to a readonly entry logger.");
    }
}
