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
package org.apache.bookkeeper.bookie;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.CRC32;

import org.apache.bookkeeper.util.ZeroBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Index page log file contents are a sequence of 64KB blocks. The only exception is that
 * the tail of the file may contain a partial block.
 *
 * <p>
 * Each block consists of a sequence of index pages.
 * <pre>
 * block := header pages* trailer?
 * header := block_type
 * index page :=
 *      checksum : uint32   // crc32c of types and data[]
 *      length   : uint16   // length of data fragment
 *      type     : uint8    // page type : FULL, MIDDLE, LAST
 *      pagelen  : uint16   // length of page
 * </pre>
 * </p>
 *
 * A page never starts within the last 9 bytes of a block (since it won't fit).
 * Any leftover bytes here form the trailer, which must consist entirely of zero bytes
 * and must be skipped by the readers.
 */
public class IndexPageLogger {

    static final Logger logger = LoggerFactory.getLogger(IndexPageLogger.class);

    // Block Types
    public static final byte DATA_BLOCK = (byte) 1;
    public static final byte META_BLOCK_FIRST = (byte) -1;
    public static final byte META_BLOCK_MIDDLE = (byte) -2;
    public static final byte META_BLOCK_LAST = (byte) -3;
    public static final byte META_BLOCK_FULL = (byte) -4;

    // Page Types
    public static enum PageType {
        FULL((byte) 1), FIRST((byte) 2), MIDDLE((byte) 3), LAST((byte) 4);

        private final byte type;

        PageType(byte type) {
            this.type = type;
        }

        public byte getType() {
            return type;
        }

        public static PageType parse(byte type) {
            switch (type) {
            case 1:
                return FULL;
            case 2:
                return FIRST;
            case 3:
                return MIDDLE;
            case 4:
                return LAST;
            default:
                throw new IllegalStateException("Unknown page type : " + type);
            }
        }
    }

    // Page ID
    public static class PageID {
        final long lid;
        final int pid;

        public PageID(long lid, int pid) {
            this.lid = lid;
            this.pid = pid;
        }

        int compareTo(PageID other) {
            if (lid < other.lid) {
                return -1;
            } else if (lid > other.lid) {
                return 1;
            } else {
                return pid - other.pid;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PageID)) {
                return false;
            }
            return 0 == compareTo((PageID) o);
        }

        @Override
        public int hashCode() {
            return (int) lid;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Page(").append(lid).append(", ").append(pid).append(")");
            return sb.toString();
        }

    }

    abstract class IndexPageLogChannel implements Closeable {

        private final long fid;
        private final FileChannel channel;

        @SuppressWarnings("resource")
        IndexPageLogChannel(long fid, String mode) throws IOException {
            this.fid = fid;
            this.channel = new RandomAccessFile(new File(indexDir, fid + INDEX_PAGE_LOG_FILE_SUFFIX), mode)
                    .getChannel();
        }

        void writeBuffer(ByteBuffer buffer) throws IOException {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }

        void readBuffer(long position, ByteBuffer buffer) throws IOException {
            while (buffer.hasRemaining()) {
                channel.read(buffer);
            }
        }

        long getFileId() {
            return fid;
        }

        FileChannel getFileChannel() {
            return channel;
        }

        @Override
        public void close() throws IOException {
            channel.force(true);
            channel.close();
        }
    }

    class ReadOnlyIndexPageLogChannel extends IndexPageLogChannel {

        ReadOnlyIndexPageLogChannel(long fid) throws IOException {
            super(fid, "r");
        }

    }

    class WritableIndexPageLogChannel extends IndexPageLogChannel {

        WritableIndexPageLogChannel(long fid) throws IOException {
            super(fid, "rw");
        }

    }

    // Current File Version
    static final int CUR_VERSION = 1;
    static final byte[] MAGIC = "INDEXPAL".getBytes();

    // Block Size
    public static final int BLOCK_SIZE = 64 * 1024; // 64K
    // Page Header : checksum (4 bytes), length (2 bytes), page type (1 byte), page length (2 bytes)
    public static final int HEADER_SIZE = 4 + 2 + 1 + 2;
    // Metadata Finalized Block : version (4 bytes), magic (8 bytes), chksum (8 bytes),
    //                            num entries (4 bytes), page size (4 bytes)
    public static final int METADATA_FINALIZED_BLOCK_SIZE = 4 + 8 + 8 + 4 + 4;
    // Position In File Mask
    public static final long POSITION_MASK = (1L << 32) - 1L;
    // Data Block Header
    static final ByteBuffer DATA_BLOCK_HEADER = ByteBuffer.wrap(new byte[] { DATA_BLOCK });
    static final ByteBuffer META_BLOCK_FIRST_HEADER = ByteBuffer.wrap(new byte[] { META_BLOCK_FIRST });
    static final ByteBuffer META_BLOCK_MID_HEADER = ByteBuffer.wrap(new byte[] { META_BLOCK_MIDDLE });
    static final ByteBuffer META_BLOCK_LAST_HEADER = ByteBuffer.wrap(new byte[] { META_BLOCK_LAST });
    static final ByteBuffer META_BLOCK_FULL_HEADER = ByteBuffer.wrap(new byte[] { META_BLOCK_FULL });
    // Max File Length
    static final long MAX_FILE_LENGTH = (1L << 32);
    // Index Entry Length
    static final int ENTRY_LENGTH = 8 + 4 + 8;
    // Index Page Log File Suffix
    static final String INDEX_PAGE_LOG_FILE_SUFFIX = ".ipl";

    // Directory to store files
    final File indexDir;
    // File Channels For Read
    final ConcurrentMap<Long, IndexPageLogChannel> sources;
    // File Channel For Write
    volatile IndexPageLogChannel dest;
    // current file id.
    volatile long destFileId;
    // current right metadata block position.
    volatile long metaBlkPosition = -1L;
    // page index
    final ConcurrentSkipListMap<PageID, Long> pages;

    // block size
    final int blockSize;
    // offset in current block
    volatile int blockOffset;
    // index page size
    final int pageSize;

    static NavigableSet<Long> getIndexPageLogFileIds(File idxDir) {
        File iplFiles[] = idxDir.listFiles();
        NavigableSet<Long> fids = new TreeSet<Long>();
        for (File f : iplFiles) {
            String name = f.getName();
            if (!name.endsWith(INDEX_PAGE_LOG_FILE_SUFFIX)) {
                continue;
            }
            String idString = name.split("\\.")[0];
            fids.add(Long.parseLong(idString, 10));
        }
        return fids;
    }

    public IndexPageLogger(File idxDir, int pageSize) throws IOException {
        this.indexDir = idxDir;
        this.sources = new ConcurrentHashMap<Long, IndexPageLogChannel>();
        this.blockSize = BLOCK_SIZE;
        this.pageSize = pageSize;
        this.pages = new ConcurrentSkipListMap<PageID, Long>();
        NavigableSet<Long> fids = getIndexPageLogFileIds(indexDir);
        // recovery log files
        recovery(fids);
        destFileId = fids.isEmpty() ? 0L : fids.last();
        createNewIndexPageLog();
    }

    public int getPageSize() {
        return pageSize;
    }

    private void recovery(NavigableSet<Long> fids) throws IOException {
        // try to find the last completed metadata blocks
        Iterator<Long> iter = fids.descendingIterator();
        while (iter.hasNext()) {
            long fid = iter.next();
            boolean success = recovery(fid);
            if (success) {
                break;
            }
        }
    }

    private boolean recovery(long fid) throws IOException {
        IndexPageLogChannel logChannel = getIndexPageLogChannel(fid);
        long offset = logChannel.getFileChannel().size();
        offset -= (offset % BLOCK_SIZE);
        boolean findHeader = false;
        boolean seenLast = false;
        byte lastBlkType = DATA_BLOCK;
        while (!findHeader && offset >= 0) {
            byte type = readBlockHeader(logChannel, offset);
            if (META_BLOCK_FULL == type) {
                findHeader = true;
                break;
            } else if (META_BLOCK_LAST == type) {
                seenLast = true;
            } else if (META_BLOCK_MIDDLE == type) {
                if (META_BLOCK_LAST != lastBlkType && META_BLOCK_MIDDLE != lastBlkType) {
                    seenLast = false;
                }
            } else if (META_BLOCK_FIRST == type) {
                if ((META_BLOCK_LAST == lastBlkType || META_BLOCK_MIDDLE == lastBlkType) && seenLast) {
                    findHeader = true;
                    break;
                } else {
                    seenLast = false;
                }
            } else {
                seenLast = false;
            }
            lastBlkType = type;
            offset -= BLOCK_SIZE;
        }
        if (findHeader) {
            logger.info("Found validate metadata blocks in position {} @ file {}.", offset, fid);
            metaBlkPosition = (fid << 32L) | (offset & POSITION_MASK);
            try {
                loadMetaBlocks(logChannel, metaBlkPosition);
            } catch (IOException ie) {
                // fail to meta blocks
                return false;
            }
            return true;
        }
        return false;
    }

    private void loadMetaBlocks(IndexPageLogChannel logChannel, long metaBlkPosition) throws IOException {
        boolean done = false;
        while (!done) {
            byte type = readBlockHeader(logChannel, metaBlkPosition);
            if (type == META_BLOCK_LAST || type == META_BLOCK_FULL) {
                // read finalized block
                ByteBuffer finalizedBlk = ByteBuffer.allocateDirect(METADATA_FINALIZED_BLOCK_SIZE);
                logChannel.readBuffer(metaBlkPosition + 1, finalizedBlk);
                finalizedBlk.flip();
                int version = finalizedBlk.getInt();
                if (CUR_VERSION != version) {
                    throw new IOException("Unknown file version " + version + ", expected version "
                            + CUR_VERSION);
                }
                byte[] magic = new byte[MAGIC.length];
                finalizedBlk.get(magic);
                for (int i = 0; i < MAGIC.length; i++) {
                    if (magic[i] != MAGIC[i]) {
                        throw new IOException("Invalid magic found : " + new String(magic));
                    }
                }
                long chksum = finalizedBlk.getLong();
                int numEntries = finalizedBlk.getInt();
                int pageSize = finalizedBlk.getInt();
                CRC32 crc32 = new CRC32();
                crc32.update(numEntries);
                crc32.update(pageSize);
                if (chksum != crc32.getValue()) {
                    throw new IOException("Corrupted finalized metadata blk : expected checksum is " + chksum
                            + " but found " + crc32.getValue());
                }
                if (pageSize != this.pageSize) {
                    throw new IOException("Invalid page size to read the index page logger : "
                            + this.pageSize + ", expected " + pageSize);
                }
                if (numEntries != pages.size()) {
                    throw new IOException("Wrong number of pages : " + pages.size() + ", expected "
                            + numEntries);
                }
                done = true;
            } else if (type == META_BLOCK_FIRST || type == META_BLOCK_MIDDLE) {
                // read meta block fragments
                ByteBuffer metaBlk = ByteBuffer.allocateDirect(BLOCK_SIZE - 1);
                logChannel.readBuffer(metaBlkPosition + 1, metaBlk);
                metaBlk.flip();
                long chksum = metaBlk.getLong();
                int numEntries = metaBlk.getInt();
                CRC32 crc32 = new CRC32();
                while (numEntries > 0) {
                    long lid = metaBlk.getLong();
                    int pid = metaBlk.getInt();
                    long address = metaBlk.getLong();
                    // update chksum
                    crc32.update((int) lid);
                    crc32.update(pid);
                    crc32.update((int) address);
                    --numEntries;
                }
                if (chksum != crc32.getValue()) {
                    throw new IOException("Invalid meta block fragment found : expected checksum " + chksum
                            + ", but found " + crc32.getValue());
                }
            } else {
                throw new IOException("Found unknown meta block fragment.");
            }
            metaBlkPosition += BLOCK_SIZE;
        }
    }

    private synchronized void saveMetaBlocks() throws IOException {
        Iterator<Entry<PageID, Long>> iter = pages.entrySet().iterator();
        Byte blockType = null;
        while (iter.hasNext()) {
            ByteBuffer blkBuf = ByteBuffer.allocateDirect(BLOCK_SIZE);
            int numEntries = 0;
            if (null == blockType) {
                blockType = META_BLOCK_FIRST;
            } else {
                blockType = META_BLOCK_MIDDLE;
            }
            // first assume meta block is full
            blkBuf.put(blockType);
            blkBuf.putLong(0);
            blkBuf.putInt(numEntries);
            CRC32 crc32 = new CRC32();
            while (iter.hasNext() && blkBuf.remaining() >= ENTRY_LENGTH) {
                Entry<PageID, Long> entry = iter.next();
                blkBuf.putLong(entry.getKey().lid);
                blkBuf.putInt(entry.getKey().pid);
                blkBuf.putLong(entry.getValue());
                // update checksum
                long position = entry.getValue();
                crc32.update((int) entry.getKey().lid);
                crc32.update(entry.getKey().pid);
                crc32.update((int) position);
                ++numEntries;
            }
            blkBuf.flip();
            // update num entries;
            blkBuf.putLong(1, crc32.getValue());
            blkBuf.putInt(9, numEntries);
            // move position to block boundary
            long position = dest.getFileChannel().position();
            if (position % BLOCK_SIZE != 0) {
                dest.getFileChannel().position(position + (BLOCK_SIZE - position % BLOCK_SIZE));
            }
            // write the meta block
            dest.writeBuffer(blkBuf);
        }
        // move position to block boundary
        long position = dest.getFileChannel().position();
        if (position % BLOCK_SIZE != 0) {
            dest.getFileChannel().position(position + (BLOCK_SIZE - position % BLOCK_SIZE));
        }
        if (null == blockType) {
            blockType = META_BLOCK_FULL;
        } else {
            blockType = META_BLOCK_LAST;
        }
        // write the finalized meta block
        ByteBuffer finalizedBlk = ByteBuffer.allocateDirect(1 + METADATA_FINALIZED_BLOCK_SIZE);
        finalizedBlk.put(blockType);
        finalizedBlk.putInt(CUR_VERSION);
        finalizedBlk.put(MAGIC);
        CRC32 crc32 = new CRC32();
        crc32.update(pages.size());
        crc32.update(pageSize);
        finalizedBlk.putLong(crc32.getValue());
        finalizedBlk.putInt(pages.size());
        finalizedBlk.putInt(pageSize);
        dest.writeBuffer(finalizedBlk);
    }

    /**
     * Flush inde page logger.
     * @throws IOException
     */
    public void flush() throws IOException {
        saveMetaBlocks();
        dest.getFileChannel().force(true);
    }

    private synchronized void createNewIndexPageLog() throws IOException {
        ++destFileId;
        dest = new WritableIndexPageLogChannel(destFileId);
        sources.put(destFileId, dest);
        blockOffset = 0;
        logger.info("Created index page log {}{}", destFileId, INDEX_PAGE_LOG_FILE_SUFFIX);
    }

    private IndexPageLogChannel getIndexPageLogChannel(long fid) throws IOException {
        IndexPageLogChannel ipl = sources.get(fid);
        if (null == ipl) {
            // open its file channel
            File iplFile = new File(indexDir, fid + INDEX_PAGE_LOG_FILE_SUFFIX);
            if (!iplFile.exists()) {
                logger.error("No index page log file found for {}{}", fid, INDEX_PAGE_LOG_FILE_SUFFIX);
                throw new FileNotFoundException("Index page log file " + fid + INDEX_PAGE_LOG_FILE_SUFFIX
                        + " is not found.");
            }
            IndexPageLogChannel newIpl = new ReadOnlyIndexPageLogChannel(fid);
            IndexPageLogChannel oldIpl = sources.putIfAbsent(fid, newIpl);
            if (null != oldIpl) {
                ipl = oldIpl;
            }
        }
        return ipl;
    }

    private void writeBlockHeader(byte type) throws IOException {
        switch (type) {
        case DATA_BLOCK:
            dest.writeBuffer(DATA_BLOCK_HEADER);
            break;
        case META_BLOCK_FIRST:
            dest.writeBuffer(META_BLOCK_FIRST_HEADER);
            break;
        case META_BLOCK_MIDDLE:
            dest.writeBuffer(META_BLOCK_MID_HEADER);
            break;
        case META_BLOCK_LAST:
            dest.writeBuffer(META_BLOCK_LAST_HEADER);
            break;
        case META_BLOCK_FULL:
            dest.writeBuffer(META_BLOCK_LAST_HEADER);
            break;
        default:
            throw new IOException("Unknown block type " + type);
        }
    }

    private static byte readBlockHeader(IndexPageLogChannel channel, long position) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1);
        channel.readBuffer(position, buf);
        buf.flip();
        return buf.get();
    }

    static void updateCRC32(CRC32 crc32, ByteBuffer bb) {
        int numInts = bb.limit() / Integer.SIZE;
        int trailingBytes = bb.limit() % Integer.SIZE;
        for (int i = 0; i < numInts; i++) {
            crc32.update(bb.getInt());
        }
        for (int i = 0; i < trailingBytes; i++) {
            crc32.update(bb.get());
        }
        bb.flip();
    }

    void ensureCapacity(int pageSize) throws IOException {
        if (dest.getFileChannel().position() > MAX_FILE_LENGTH) {
            // flush current log
            flush();
            // rotate to next log file
            createNewIndexPageLog();
        }
    }

    /**
     * Write a given page.
     *
     * @param page
     *          index page.
     */
    public synchronized void writeIndexPage(long lid, int pid, ByteBuffer page) throws IOException {
        // ensure we had enought room for an index page
        ensureCapacity(page.limit());
        // write block header, if it is the first object in this block
        if (blockOffset == 0) {
            writeBlockHeader(DATA_BLOCK);
        }
        int pageLen = page.remaining();
        if (pid >= 0 && pageSize != pageLen) {
            throw new IOException("Invalid page " + pid + " @ ledger " + lid);
        }
        // fragment the page if necessary and emit it.
        boolean success = false;
        boolean begin = true;
        long position = -1L;
        do {
            int leftover = blockSize - blockOffset;
            if (leftover < HEADER_SIZE) {
                // fill trailer
                ByteBuffer trailer = ByteBuffer.allocateDirect(leftover);
                ZeroBuffer.put(trailer);
                dest.writeBuffer(trailer);
                writeBlockHeader(DATA_BLOCK);
                blockOffset = 1;
            }
            if (-1L == position) {
                position = dest.getFileChannel().position();
            }

            int available = blockSize - blockOffset - HEADER_SIZE;
            boolean end = page.remaining() < available;
            int fragmentLen = end ? page.remaining() : available;
            PageType type;
            if (begin && end) {
                type = PageType.FULL;
            } else if (begin) {
                type = PageType.FIRST;
            } else if (end) {
                type = PageType.LAST;
            } else {
                type = PageType.MIDDLE;
            }
            ByteBuffer fragmentBuf = page.slice();
            fragmentBuf.limit(fragmentLen);
            doWriteIndexPage(type, page, fragmentLen, pageLen);
            // move position of original obj buffer
            page.position(page.position() + fragmentLen);
            begin = false;
        } while (!success && page.hasRemaining());
        long pagePos = (destFileId << 32L) | (position & POSITION_MASK);
        // update page index
        pages.put(new PageID(lid, pid), pagePos);
    }

    private void doWriteIndexPage(PageType type, ByteBuffer data, int dataLen, int pageLen)
            throws IOException {
        Preconditions.checkArgument(dataLen < 0xffff);
        Preconditions.checkArgument(pageLen < 0xffff);
        // format I/O header
        byte[] header = new byte[HEADER_SIZE];
        header[4] = (byte) (dataLen & 0xff);
        header[5] = (byte) (dataLen >> 8);
        header[6] = type.getType();
        header[7] = (byte) (pageLen & 0xff);
        header[8] = (byte) (pageLen >> 8);
        // compute the crc32
        CRC32 chksum = new CRC32();
        updateCRC32(chksum, data);
        chksum.update(type.getType());
        chksum.update(pageLen);
        int chksumVal = (int) chksum.getValue();
        // form header buffer
        ByteBuffer headerBuf = ByteBuffer.wrap(header);
        headerBuf.putInt(chksumVal);
        headerBuf.clear();
        // update header
        dest.writeBuffer(headerBuf);
        // update the data
        dest.writeBuffer(data);
        // move block offset
        blockOffset += (HEADER_SIZE + dataLen);
    }

    /**
     * Read index page.
     *
     * @param lid
     *          ledger id
     * @param pid
     *          page id
     * @param page
     *          page buffer.
     * @throws IOException
     */
    public ByteBuffer readIndexPage(long lid, int pid) throws IOException {
        PageID pageId = new PageID(lid, pid);
        Long address = pages.get(pageId);
        if (null == address) {
            return null;
        }
        long position = address & POSITION_MASK;
        long fileid = (address >> 32L);
        IndexPageLogChannel channel = getIndexPageLogChannel(fileid);
        return doReadIndexPage(pageId, channel, position);
    }

    private ByteBuffer doReadIndexPage(PageID pid, IndexPageLogChannel reader, long position)
            throws IOException {
        boolean success = false;
        boolean seenFirst = false;
        PageType prevPageType = null;
        ByteBuffer buffer = null;
        ByteBuffer bufferForCRC32 = null;
        while (!success) {
            ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
            reader.readBuffer(position, headerBuf);
            headerBuf.flip();
            position += HEADER_SIZE;
            int chksum = headerBuf.getInt();
            int dataLen = (headerBuf.get() & 0xff) | (headerBuf.get() << 8);
            PageType pageType = PageType.parse(headerBuf.get());
            int pageLen = (headerBuf.get() & 0xff) | (headerBuf.get() << 8);
            if (buffer == null) {
                buffer = ByteBuffer.allocateDirect(pageLen);
                bufferForCRC32 = buffer.duplicate();
            }
            int pos = buffer.position();
            buffer.limit(pos + dataLen);
            reader.readBuffer(position + HEADER_SIZE, buffer);
            // compute crc32
            bufferForCRC32.position(pos);
            bufferForCRC32.limit(pos + dataLen);
            CRC32 crc32 = new CRC32();
            updateCRC32(crc32, bufferForCRC32);
            crc32.update(pageType.getType());
            crc32.update(pageLen);
            if (chksum == crc32.getValue()) {
                throw new IOException("Invalid CRC32 page fragment found in position " + position + " for "
                        + pid);
            }
            switch (pageType) {
            case FULL:
                if (null != prevPageType) {
                    throw new IOException("Corrupted FULL page fragment found in position " + position
                            + " for " + pid);
                }
                success = true;
                buffer.flip();
                return buffer;
            case FIRST:
                if (seenFirst && null != prevPageType) {
                    throw new IOException("Corrupted FIRST page fragment found in position " + position
                            + " for " + pid);
                }
                seenFirst = true;
                break;
            case LAST:
                if (!seenFirst || (PageType.FIRST != prevPageType && PageType.MIDDLE != prevPageType)) {
                    throw new IOException("Corrupted LAST page fragment found in position " + position
                            + " for " + pid);
                }
                success = true;
                buffer.flip();
                return buffer;
            case MIDDLE:
                if (!seenFirst || (PageType.FIRST != prevPageType && PageType.MIDDLE != prevPageType)) {
                    throw new IOException("Corrupted MID page fragment found in position " + position
                            + " for " + pid);
                }
                break;
            default:
                throw new IOException("Unknown page fragment " + pageType + " found in position " + position
                        + " for " + pid);
            }
            prevPageType = pageType;
            // move on the next fragment
        }
        throw new IOException("Corrupted index page found for " + pid);
    }

}
