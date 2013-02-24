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
package org.apache.bookkeeper.meta;

import java.io.IOException;

import org.apache.bookkeeper.client.sl.SuperLedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.sl.SuperLedgerProtocol.StatusCode;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class ZkSuperLedgerManager implements SuperLedgerManager {

    static final Logger LOG = LoggerFactory.getLogger(ZkSuperLedgerManager.class);

    protected final AbstractConfiguration conf;
    protected final ZooKeeper zk;
    protected final String ledgerRootPath;

    ZkSuperLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        this.conf = conf;
        this.zk = zk;
        this.ledgerRootPath = conf.getZkSuperLedgersRootPath();
    }

    @Override
    public void close() throws IOException {
        // do nothing now.
    }

    private String getSuperLedgerPath(ByteString ledgerName) {
        return ledgerRootPath + ledgerName.toStringUtf8();
    }

    @Override
    public void writeSuperLedgerMetadata(final ByteString ledgerName, final SuperLedgerMetadata metadata,
            final SuperLedgerMetadataCallback<Void> callback) {
        Version v = metadata.getVersion();
        if (Version.NEW == v) {
            createSuperLedgerMetadata(ledgerName, metadata, callback);
            return;
        }
        final ZkVersion zv = (ZkVersion) v;
        zk.setData(getSuperLedgerPath(ledgerName), metadata.serialize(), zv.getZnodeVersion(),
                new StatCallback() {

                    @Override
                    public void processResult(int rc, String path, Object ctx, Stat stat) {
                        if (KeeperException.Code.OK.intValue() == rc) {
                            // update metadata version
                            metadata.setVersion(zv.setZnodeVersion(stat.getVersion()));
                            callback.complete(StatusCode.EOK, null);
                        } else if (KeeperException.Code.BADVERSION.intValue() == rc) {
                            callback.complete(StatusCode.EBADMETADATAVERSION, null);
                        } else if (KeeperException.Code.NONODE.intValue() == rc) {
                            callback.complete(StatusCode.ENOSUPERLEDGER, null);
                        } else {
                            LOG.warn(
                                    "Conditional update super ledger metadata for "
                                            + ledgerName.toStringUtf8() + " failed: ",
                                    KeeperException.Code.get(rc));
                            callback.complete(StatusCode.EMETADATAEXCEPTION, null);
                        }
                    }

                }, null);
    }

    private void createSuperLedgerMetadata(final ByteString ledgerName, final SuperLedgerMetadata metadata,
            final SuperLedgerMetadataCallback<Void> callback) {
        ZkUtils.createFullPathOptimistic(zk, getSuperLedgerPath(ledgerName), metadata.serialize(),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new StringCallback() {

                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        if (KeeperException.Code.OK.intValue() == rc) {
                            // update version
                            metadata.setVersion(new ZkVersion(0));
                            callback.complete(StatusCode.EOK, null);
                        } else if (KeeperException.Code.NODEEXISTS.intValue() == rc) {
                            LOG.warn("Could not create znode for super ledger " + ledgerName.toStringUtf8()
                                    + " since it already exists : ", KeeperException.Code.get(rc));
                            callback.complete(StatusCode.EBADMETADATAVERSION, null);
                        } else {
                            LOG.error("Could not create znode for super ledger " + ledgerName.toStringUtf8()
                                    + " : ", KeeperException.create(KeeperException.Code.get(rc)));
                            callback.complete(StatusCode.EMETADATAEXCEPTION, null);
                        }
                    }

                }, null);
    }

    @Override
    public void readSuperLedgerMetadata(final ByteString ledgerName,
            final SuperLedgerMetadataCallback<SuperLedgerMetadata> callback) {
        zk.getData(getSuperLedgerPath(ledgerName), false, new DataCallback() {

            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (KeeperException.Code.NONODE.intValue() == rc) {
                    LOG.info("No super ledger {} found.", ledgerName.toStringUtf8());
                    callback.complete(StatusCode.ENOSUPERLEDGER, null);
                    return;
                }
                if (KeeperException.Code.OK.intValue() != rc) {
                    LOG.error(
                            "Could not read metadata for super ledger " + ledgerName.toStringUtf8() + " : ",
                            KeeperException.create(KeeperException.Code.get(rc)));
                    callback.complete(StatusCode.EMETADATAEXCEPTION, null);
                    return;
                }
                SuperLedgerMetadata metadata;
                try {
                    metadata = SuperLedgerMetadata.parseConfig(data, new ZkVersion(stat.getVersion()));
                } catch (IOException e) {
                    LOG.error("Could not parse super ledger metadata for " + ledgerName.toStringUtf8()
                            + " : ", e);
                    callback.complete(StatusCode.EMETADATAEXCEPTION, null);
                    return;
                }
                callback.complete(StatusCode.EOK, metadata);
            }

        }, null);
    }

}
