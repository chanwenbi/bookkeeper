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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SuperLedgerMetadataFormat;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.SuperLedgerMetadataFormat.Epoch;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.versioning.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

public class SuperLedgerMetadata {

    static final Logger LOG = LoggerFactory.getLogger(SuperLedgerMetadata.class);

    private int ensembleSize;
    private int writeQuorumSize;
    private int ackQuorumSize;

    ArrayList<InetSocketAddress> ensemble = new ArrayList<InetSocketAddress>();
    volatile Version version = Version.NEW;

    private SuperLedgerMetadataFormat.DigestType digestType;
    private ByteString password;

    private long epochTime;
    private int lastEpoch = -1;
    private SortedMap<Integer, Long> epochs = new TreeMap<Integer, Long>();

    public SuperLedgerMetadata(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            BookKeeper.DigestType digestType, ByteString password, long epochTime) {
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;

        this.digestType = digestType.equals(BookKeeper.DigestType.MAC) ? SuperLedgerMetadataFormat.DigestType.HMAC
                : SuperLedgerMetadataFormat.DigestType.CRC32;
        this.password = password;
        this.epochTime = epochTime;
    }

    private SuperLedgerMetadata() {
        this(0, 0, 0, BookKeeper.DigestType.MAC, ByteString.copyFrom(new byte[0]), 0);
    }

    /**
     * Get the ensemble for super ledger.
     * 
     * @return the corresponding bookie ensembles that store the entries.
     */
    public ArrayList<InetSocketAddress> getEnsemble() {
        return ensemble;
    }

    public int getEnsembleSize() {
        return ensembleSize;
    }

    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    public int getAckQuorumSize() {
        return ackQuorumSize;
    }

    ByteString getPassword() {
        return password;
    }

    BookKeeper.DigestType getDigestType() {
        if (digestType.equals(LedgerMetadataFormat.DigestType.HMAC)) {
            return BookKeeper.DigestType.MAC;
        } else {
            return BookKeeper.DigestType.CRC32;
        }
    }

    public long getEpochTime() {
        return epochTime;
    }

    SuperLedgerMetadata bumpEpoch() {
        ++lastEpoch;
        epochs.put(lastEpoch, System.currentTimeMillis());
        return this;
    }

    public int getEpoch() {
        return lastEpoch;
    }

    public Long getEpochCreationTime(int epoch) {
        return epochs.get(epoch);
    }

    /**
     * Generates a byte array of this object
     * 
     * @return the metadata serialized into a byte array
     */
    public byte[] serialize() {
        SuperLedgerMetadataFormat.Builder builder = SuperLedgerMetadataFormat.newBuilder();
        builder.setEnsembleSize(ensembleSize).setWriteQuorumSize(writeQuorumSize)
                .setAckQuorumSize(ackQuorumSize).setEpochTime(epochTime).setDigestType(digestType)
                .setPassword(password);

        for (InetSocketAddress addr : ensemble) {
            builder.addEnsembleMember(addr.getAddress().getHostAddress() + ":" + addr.getPort());
        }
        for (Map.Entry<Integer, Long> entry : epochs.entrySet()) {
            Epoch.Builder epoch = Epoch.newBuilder().setEpoch(entry.getKey()).setCtime(entry.getValue());
            builder.addEpochs(epoch);
        }
        return TextFormat.printToString(builder.build()).getBytes(UTF_8);
    }

    /**
     * Parses a given byte array and transforms into a SuperLedgerMetadata
     * object
     * 
     * @param bytes
     *            byte array to parse
     * @param version
     *            version of the ledger metadata
     * @return LedgerConfig
     * @throws IOException
     *             if the given byte[] cannot be parsed
     */
    public static SuperLedgerMetadata parseConfig(byte[] bytes, Version version) throws IOException {
        SuperLedgerMetadata lc = new SuperLedgerMetadata();
        lc.version = version;

        String config = new String(bytes, UTF_8);

        LOG.debug("Parsing SuperLedger Config: {}", config);

        SuperLedgerMetadataFormat.Builder builder = SuperLedgerMetadataFormat.newBuilder();
        TextFormat.merge(config, builder);
        SuperLedgerMetadataFormat data = builder.build();
        lc.writeQuorumSize = data.getWriteQuorumSize();
        lc.ackQuorumSize = data.getAckQuorumSize();
        lc.ensembleSize = data.getEnsembleSize();
        lc.digestType = data.getDigestType();
        lc.password = data.getPassword();
        lc.epochTime = data.getEpochTime();
        
        for (String member : data.getEnsembleMemberList()) {
            lc.ensemble.add(StringUtils.parseAddr(member));
        }
        for (Epoch epoch : data.getEpochsList()) {
            lc.epochs.put(epoch.getEpoch(), epoch.getCtime());
        }
        if (!lc.epochs.isEmpty()) {
            lc.lastEpoch = lc.epochs.lastKey();
        } else {
            lc.lastEpoch = -1;
        }
        return lc;
    }

    /**
     * Updates the version of this metadata.
     * 
     * @param v
     *            Version
     */
    public void setVersion(Version v) {
        this.version = v;
    }

    /**
     * Returns the last version.
     * 
     * @return version
     */
    public Version getVersion() {
        return this.version;
    }

}
