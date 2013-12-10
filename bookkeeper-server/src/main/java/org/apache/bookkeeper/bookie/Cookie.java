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

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.DataFormats.CookieFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import com.google.protobuf.TextFormat;

/**
 * When a bookie starts for the first time it generates  a cookie, and stores
 * the cookie in zookeeper as well as in the each of the local filesystem
 * directories it uses. This cookie is used to ensure that for the life of the
 * bookie, its configuration stays the same. If any of the bookie directories
 * becomes unavailable, the bookie becomes unavailable. If the bookie changes
 * port, it must also reset all of its data.
 *
 * This is done to ensure data integrity. Without the cookie a bookie could
 * start with one of its ledger directories missing, so data would be missing,
 * but the bookie would be up, so the client would think that everything is ok
 * with the cluster. It's better to fail early and obviously.
 */
class Cookie {
    private final static Logger LOG = LoggerFactory.getLogger(Cookie.class);

    static class Mutator {
        private String bookieHost = null;
        private String journalDir = null;
        private String[] ledgerDirs = null;
        private String instanceId = null;
        private final int znodeVersion;

        Mutator(Cookie cookie) {
            this.bookieHost = cookie.bookieHost;
            this.journalDir = cookie.journalDir;
            this.ledgerDirs = cookie.ledgerDirs;
            this.instanceId = cookie.instanceId;
            this.znodeVersion = cookie.znodeVersion;
        }

        Mutator setBookieHost(String bookieHost) {
            this.bookieHost = bookieHost;
            return this;
        }

        Mutator setJournalDir(String journalDir) {
            this.journalDir = journalDir;
            return this;
        }

        Mutator setLedgerDirs(String[] ledgerDirs) {
            this.ledgerDirs = ledgerDirs;
            return this;
        }

        Mutator removeLedgerDir(String ledgerDir) {
            Preconditions.checkNotNull(ledgerDirs);
            List<String> result = new ArrayList<String>(ledgerDirs.length);
            for (String dir : ledgerDirs) {
                if (dir.equals(ledgerDir)) {
                    continue;
                }
                result.add(dir);
            }
            this.ledgerDirs = result.toArray(new String[result.size()]);
            return this;
        }

        Mutator addLedgerDir(String ledgerDir) {
            Preconditions.checkNotNull(ledgerDirs);
            String[] newLedgerDirs = new String[ledgerDirs.length + 1];
            System.arraycopy(ledgerDirs, 0, newLedgerDirs, 0, ledgerDirs.length);
            newLedgerDirs[ledgerDirs.length] = ledgerDir;
            this.ledgerDirs = newLedgerDirs;
            return this;
        }

        Mutator replaceLedgerDir(String oldDir, String newDir) {
            Preconditions.checkNotNull(ledgerDirs);
            List<String> result = new ArrayList<String>(ledgerDirs.length);
            for (String dir : ledgerDirs) {
                if (dir.equals(oldDir)) {
                    result.add(newDir);
                } else {
                    result.add(dir);
                }
            }
            this.ledgerDirs = result.toArray(new String[result.size()]);
            return this;
        }

        Mutator setInstanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        Cookie build() {
            Preconditions.checkNotNull(bookieHost);
            Preconditions.checkNotNull(journalDir);
            Preconditions.checkNotNull(ledgerDirs);
            return new Cookie(bookieHost, journalDir, ledgerDirs, CURRENT_COOKIE_LAYOUT_VERSION)
                    .setInstanceId(instanceId).setZkVersion(znodeVersion);
        }
    }

    static final int CURRENT_COOKIE_LAYOUT_VERSION = 4;
    private final int layoutVersion;
    private final String bookieHost;
    private final String journalDir;
    private final String[] ledgerDirs;
    private String instanceId = null;
    private int znodeVersion = -1;

    private Cookie(String bookieHost, String journalDir, String[] ledgerDirs,
                   int layoutVersion) {
        this.bookieHost = bookieHost;
        this.journalDir = journalDir;
        this.ledgerDirs = ledgerDirs;
        this.layoutVersion = layoutVersion;
    }

    public Mutator newMutator() {
        return new Mutator(this);
    }

    public void verify(Cookie c) throws BookieException.InvalidCookieException {
        String errMsg;
        if (c.layoutVersion < 3 && c.layoutVersion != layoutVersion) {
            errMsg = "Cookie is of too old version " + c.layoutVersion;
            LOG.error(errMsg);
            throw new BookieException.InvalidCookieException(errMsg);
        } else if (!(c.layoutVersion >= 3 && c.bookieHost.equals(bookieHost)
                && c.journalDir.equals(journalDir) && compareDirs(c.ledgerDirs, ledgerDirs))) {
            errMsg = "Cookie [" + this + "] is not matching with [" + c + "]";
            throw new BookieException.InvalidCookieException(errMsg);
        } else if ((instanceId == null && c.instanceId != null)
                || (instanceId != null && !instanceId.equals(c.instanceId))) {
            // instanceId should be same in both cookies
            errMsg = "instanceId " + instanceId
                    + " is not matching with " + c.instanceId;
            throw new BookieException.InvalidCookieException(errMsg);
        }
    }

    public String toString() {
        if (layoutVersion <= 3) {
            return toStringVersion3();
        }
        CookieFormat.Builder builder = CookieFormat.newBuilder();
        builder.setBookieHost(bookieHost);
        builder.setJournalDir(journalDir);
        builder.setLedgerDirs(serializeDirs(ledgerDirs));
        if (null != instanceId) {
            builder.setInstanceId(instanceId);
        }
        StringBuilder b = new StringBuilder();
        b.append(CURRENT_COOKIE_LAYOUT_VERSION).append("\n");
        b.append(TextFormat.printToString(builder.build()));
        return b.toString();
    }

    private String toStringVersion3() {
        StringBuilder b = new StringBuilder();
        b.append(CURRENT_COOKIE_LAYOUT_VERSION).append("\n")
            .append(bookieHost).append("\n")
            .append(journalDir).append("\n")
            .append(serializeDirs(ledgerDirs)).append("\n");
        return b.toString();
    }

    private static Cookie parse(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (null == line) {
            throw new EOFException("Exception in parsing cookie");
        }
        int layoutVersion;
        String bookieHost = null;
        String journalDir = null;
        String ledgerDirs = null;
        String instanceId = null;
        try {
            layoutVersion = Integer.parseInt(line.trim());
        } catch (NumberFormatException e) {
            throw new IOException("Invalid string '" + line.trim()
                    + "', cannot parse cookie.");
        }
        if (layoutVersion == 3) {
            bookieHost = reader.readLine();
            journalDir = reader.readLine();
            ledgerDirs = reader.readLine();
        } else if (layoutVersion >= 4) {
            CookieFormat.Builder builder = CookieFormat.newBuilder();
            TextFormat.merge(reader, builder);
            CookieFormat data = builder.build();
            bookieHost = data.getBookieHost();
            journalDir = data.getJournalDir();
            ledgerDirs = data.getLedgerDirs();
            // Since InstanceId is optional
            if (null != data.getInstanceId() && !data.getInstanceId().isEmpty()) {
                instanceId = data.getInstanceId();
            }
        }
        return new Cookie(bookieHost, journalDir, deserializeDirs(ledgerDirs), layoutVersion)
                .setInstanceId(instanceId);
    }

    void writeToDirectory(File directory) throws IOException {
        File versionFile = new File(directory,
                BookKeeperConstants.VERSION_FILENAME);

        FileOutputStream fos = new FileOutputStream(versionFile);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
            bw.write(toString());
        } finally {
            if (bw != null) {
                bw.close();
            }
            fos.close();
        }
    }

    void writeToZooKeeper(ZooKeeper zk, ServerConfiguration conf)
            throws KeeperException, InterruptedException, UnknownHostException {
        writeToZooKeeper(zk, getZkPath(conf));
    }

    void writeToZooKeeper(ZooKeeper zk, String bookieCookiePath)
            throws KeeperException, InterruptedException {
        byte[] data = toString().getBytes(UTF_8);
        if (znodeVersion != -1) {
            zk.setData(bookieCookiePath, data, znodeVersion);
        } else {
            ZkUtils.createFullPathOptimistic(zk, bookieCookiePath, data,
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            this.znodeVersion = 0;
        }
    }

    void deleteFromZooKeeper(ZooKeeper zk, ServerConfiguration conf)
            throws KeeperException, InterruptedException, UnknownHostException {
        String zkPath = getZkPath(conf);
        if (znodeVersion != -1) {
            zk.delete(zkPath, znodeVersion);
        }
        znodeVersion = -1;
    }

    void renameInZooKeeper(ZooKeeper zk, String oldPath, String newPath)
            throws KeeperException, InterruptedException {
        byte[] data = toString().getBytes(UTF_8);
        Transaction txn = zk.transaction();
        txn.delete(oldPath, znodeVersion);
        txn.create(newPath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.commit();
    }

    static Cookie generateCookie(ServerConfiguration conf)
            throws UnknownHostException {
        int layoutVersion = CURRENT_COOKIE_LAYOUT_VERSION;
        String bookieHost = StringUtils.addrToString(Bookie.getBookieAddress(conf));
        String journalDir = conf.getJournalDirName();
        return new Cookie(bookieHost, journalDir, conf.getLedgerDirNames(), layoutVersion);
    }

    static Cookie readFromZooKeeper(ZooKeeper zk, ServerConfiguration conf)
            throws KeeperException, InterruptedException, IOException, UnknownHostException {
        String zkPath = getZkPath(conf);

        Stat stat = zk.exists(zkPath, false);
        byte[] data = zk.getData(zkPath, false, stat);
        BufferedReader reader = new BufferedReader(new StringReader(new String(data, UTF_8)));
        try {
            Cookie c = parse(reader);
            c.znodeVersion = stat.getVersion();
            return c;
        } finally {
            reader.close();
        }
    }

    static Cookie readFromDirectory(File directory) throws IOException {
        File versionFile = new File(directory,
                BookKeeperConstants.VERSION_FILENAME);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(versionFile), UTF_8));
        try {
            return parse(reader);
        } finally {
            reader.close();
        }
    }

    Cookie setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    Cookie setZkVersion(int zkVersion) {
        this.znodeVersion = zkVersion;
        return this;
    }

    private static boolean compareDirs(String[] dirs1, String[] dirs2) {
        if (dirs1 == null && dirs2 == null) {
            return true;
        } else if (dirs1 != null && dirs2 != null) {
            if (dirs1.length != dirs2.length) {
                return false;
            }
            for (int i = 0; i < dirs1.length; i++) {
                if (!dirs1[i].equals(dirs2[i])) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private static String serializeDirs(String[] dirs) {
        StringBuilder b = new StringBuilder();
        b.append(dirs.length);
        for (String d : dirs) {
            b.append("\t").append(d);
        }
        return b.toString();
    }

    private static String[] deserializeDirs(String line) throws IOException {
        String[] parts = org.apache.commons.lang.StringUtils.split(line, '\t');
        if (null == parts || parts.length <= 0) {
            throw new IOException("Invalid dirs : " + line);
        }
        int numDirs = Integer.parseInt(parts[0]);
        if (parts.length != numDirs + 1) {
            throw new IOException("Invalid dirs : " + line);
        }
        String[] dirs = new String[numDirs];
        System.arraycopy(parts, 1, dirs, 0, numDirs);
        return dirs;
    }

    private static String getZkPath(ServerConfiguration conf)
            throws UnknownHostException {
        String bookieCookiePath = conf.getZkLedgersRootPath() + "/"
                + BookKeeperConstants.COOKIE_NODE;
        return bookieCookiePath + "/" + StringUtils.addrToString(Bookie.getBookieAddress(conf));
    }
}
