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
import java.util.BitSet;

import org.apache.bookkeeper.proto.BookieProtocol;

/**
 * This is a page in the LedgerCache. It holds the locations
 * (entrylogfile, offset) for entry ids.
 */
public class LedgerEntryPage {
    private final int pageSize;
    private final int entriesPerPage;
    private long ledger = -1;
    private long firstEntry = BookieProtocol.INVALID_ENTRY_ID;
    private final ByteBuffer page;
    private boolean clean = true;
    private boolean pinned = false;
    private int useCount;
    private int version;

    // BitSet to indicate the status waiting its entry is synced
    // to a journal
    private BitSet unsyncBits;

    public LedgerEntryPage(int pageSize, int entriesPerPage) {
        this.pageSize = pageSize;
        this.entriesPerPage = entriesPerPage;
        this.unsyncBits = new BitSet(entriesPerPage);
        page = ByteBuffer.allocateDirect(pageSize);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getLedger());
        sb.append('@');
        sb.append(getFirstEntry());
        sb.append(clean ? " clean " : " dirty ");
        sb.append(" unsync entries bitmap : ");
        sb.append(unsyncBits);
        sb.append(useCount);
        return sb.toString();
    }

    synchronized boolean isFlushable() {
        return unsyncBits.isEmpty();     
    }

    synchronized void entrySynced(int idx) {
        unsyncBits.clear(idx);
    }

    synchronized void waitEntrySynced(int idx) {
        unsyncBits.set(idx, true);
    }

    synchronized public void usePage() {
        useCount++;
    }
    synchronized public void pin() {
        pinned = true;
    }
    synchronized public void unpin() {
        pinned = false;
    }
    synchronized public boolean isPinned() {
        return pinned;
    }
    synchronized public void releasePage() {
        useCount--;
        if (useCount < 0) {
            throw new IllegalStateException("Use count has gone below 0");
        }
    }
    synchronized private void checkPage() {
        if (useCount <= 0) {
            throw new IllegalStateException("Page not marked in use");
        }
    }
    @Override
    public boolean equals(Object other) {
        if (other instanceof LedgerEntryPage) {
            LedgerEntryPage otherLEP = (LedgerEntryPage) other;
            return otherLEP.getLedger() == getLedger() && otherLEP.getFirstEntry() == getFirstEntry();
        } else {
            return false;
        }
    }
    @Override
    public int hashCode() {
        return (int)getLedger() ^ (int)(getFirstEntry());
    }
    synchronized void setClean(int versionOfCleaning) {
        this.clean = (versionOfCleaning == version);
    }
    synchronized boolean isClean() {
        return clean;
    }
    synchronized public void setOffset(long offset, int position) {
        checkPage();
        version++;
        this.clean = false;
        page.putLong(position, offset);
    }
    synchronized public long getOffset(int position) {
        checkPage();
        return page.getLong(position);
    }
    static final byte zeroPage[] = new byte[64*1024];
    synchronized public void zeroPage() {
        checkPage();
        page.clear();
        page.put(zeroPage, 0, page.remaining());
        clean = true;
        unsyncBits.clear();
    }
    synchronized public void readPage(FileInfo fi) throws IOException {
        checkPage();
        page.clear();
        while(page.remaining() != 0) {
            if (fi.read(page, getFirstEntry()*8) <= 0) {
                throw new IOException("Short page read of ledger " + getLedger() + " tried to get " + page.capacity() + " from position " + getFirstEntry()*8 + " still need " + page.remaining());
            }
        }
        clean = true;
        unsyncBits.clear();
    }
    synchronized public ByteBuffer getPageToWrite() {
        checkPage();
        page.clear();
        return page;
    }
    synchronized void setLedger(long ledger) {
        this.ledger = ledger;
    }
    synchronized long getLedger() {
        return ledger;
    }
    synchronized int getVersion() {
        return version;
    }
    synchronized void setFirstEntry(long firstEntry) {
        if (firstEntry % entriesPerPage != 0) {
            throw new IllegalArgumentException(firstEntry + " is not a multiple of " + entriesPerPage);
        }
        this.firstEntry = firstEntry;
    }
    synchronized long getFirstEntry() {
        return firstEntry;
    }
    public synchronized boolean inUse() {
        return useCount > 0;
    }
    public synchronized long getLastEntry() {
        for(int i = entriesPerPage - 1; i >= 0; i--) {
            if (getOffset(i*8) > 0) {
                return i + firstEntry;
            }
        }
        return 0;
    }
}
