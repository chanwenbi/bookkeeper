/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server.snitch.helix;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.hedwig.StubCallback;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionState;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.meta.SubscriptionDataManager;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.Either;
import org.apache.hedwig.util.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestHelixLocalSubscriptionStorage extends TestCase {

    SubscriptionDataManager subManager;
    File leveldbDir;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        leveldbDir = FileUtils.createTempDirectory("hub-subscriptiondb", "test");
        ServerConfiguration conf = new ServerConfiguration() {

            @Override
            public String getLeveldbSubscriptionDBPath() {
                try {
                    return leveldbDir.getCanonicalPath().toString();
                } catch (IOException e) {
                    return null;
                }
            }

        };
        subManager = new HelixLocalSubscriptionStorage(conf, new OrderedSafeExecutor(conf.getLeveldbNumIOWorkers()));
    }

    @Override
    protected void tearDown() throws Exception {
        if (null != leveldbDir) {
            org.apache.commons.io.FileUtils.deleteDirectory(leveldbDir);
        }
        super.tearDown();
    }

    @Test(timeout = 60000)
    public void testSubscriptionData() throws Exception {
        ByteString topic = ByteString.copyFromUtf8("testSubscriptionData");
        ByteString subid = ByteString.copyFromUtf8("mysub");

        final StubCallback<Version> callback = new StubCallback<Version>();
        StubCallback<Versioned<SubscriptionData>> readCallback = new StubCallback<Versioned<SubscriptionData>>();
        StubCallback<Map<ByteString, Versioned<SubscriptionData>>> subsCallback = new StubCallback<Map<ByteString, Versioned<SubscriptionData>>>();

        subManager.readSubscriptionData(topic, subid, readCallback, null);
        Either<Versioned<SubscriptionData>, PubSubException> readRes = readCallback.queue.take();
        Assert.assertEquals("Found inconsistent subscription state", null, readRes.left());
        Assert.assertEquals("Should not fail with PubSubException", null, readRes.right());

        // read non-existed subscription state
        subManager.readSubscriptions(topic, subsCallback, null);
        Either<Map<ByteString, Versioned<SubscriptionData>>, PubSubException> res = subsCallback.queue.take();
        Assert.assertEquals("Found more than 0 subscribers", 0, res.left().size());
        Assert.assertEquals("Should not fail with PubSubException", null, res.right());

        // update non-existed subscription state
        if (subManager.isPartialUpdateSupported()) {
            subManager.updateSubscriptionData(topic, subid, SubscriptionData.getDefaultInstance(), Version.ANY,
                    callback, null);
        } else {
            subManager.replaceSubscriptionData(topic, subid, SubscriptionData.getDefaultInstance(), Version.ANY,
                    callback, null);
        }
        Assert.assertTrue("Should fail to update a non-existed subscriber with PubSubException", callback.queue.take()
                .right() instanceof PubSubException.NoSubscriptionStateException);

        Callback<Void> voidCallback = new Callback<Void>() {
            @Override
            public void operationFinished(Object ctx, Void resultOfOperation) {
                callback.operationFinished(ctx, null);
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                callback.operationFailed(ctx, exception);
            }
        };

        // delete non-existed subscription state
        subManager.deleteSubscriptionData(topic, subid, Version.ANY, voidCallback, null);
        Assert.assertTrue("Should fail to delete a non-existed subscriber with PubSubException", callback.queue.take()
                .right() instanceof PubSubException.NoSubscriptionStateException);

        long seqId = 10;
        MessageSeqId.Builder builder = MessageSeqId.newBuilder();
        builder.setLocalComponent(seqId);
        MessageSeqId msgId = builder.build();

        SubscriptionState.Builder stateBuilder = SubscriptionState.newBuilder(SubscriptionState.getDefaultInstance())
                .setMsgId(msgId);
        SubscriptionData data = SubscriptionData.newBuilder().setState(stateBuilder).build();

        // create a subscription state
        subManager.createSubscriptionData(topic, subid, data, callback, null);
        Either<Version, PubSubException> cbResult = callback.queue.take();
        Version v1 = cbResult.left();
        Assert.assertEquals("Should not fail with PubSubException", null, cbResult.right());

        // read subscriptions
        subManager.readSubscriptions(topic, subsCallback, null);
        res = subsCallback.queue.take();
        Assert.assertEquals("Should find just 1 subscriber", 1, res.left().size());
        Assert.assertEquals("Should not fail with PubSubException", null, res.right());
        Versioned<SubscriptionData> versionedSubData = res.left().get(subid);
        Assert.assertEquals(Version.Occurred.CONCURRENTLY, v1.compare(versionedSubData.getVersion()));
        SubscriptionData imss = versionedSubData.getValue();
        Assert.assertEquals("Found inconsistent subscription state", data, imss);
        Assert.assertEquals("Found inconsistent last consumed seq id", seqId, imss.getState().getMsgId()
                .getLocalComponent());

        // move consume seq id
        seqId = 99;
        builder = MessageSeqId.newBuilder();
        builder.setLocalComponent(seqId);
        msgId = builder.build();

        stateBuilder = SubscriptionState.newBuilder(data.getState()).setMsgId(msgId);
        data = SubscriptionData.newBuilder().setState(stateBuilder).build();

        // update subscription state
        if (subManager.isPartialUpdateSupported()) {
            subManager.updateSubscriptionData(topic, subid, data, versionedSubData.getVersion(), callback, null);
        } else {
            subManager.replaceSubscriptionData(topic, subid, data, versionedSubData.getVersion(), callback, null);
        }
        cbResult = callback.queue.take();
        Assert.assertEquals("Fail to update a subscription state", null, cbResult.right());
        Version v2 = cbResult.left();
        // read subscription state
        subManager.readSubscriptionData(topic, subid, readCallback, null);
        Assert.assertEquals("Found inconsistent subscription state", data, readCallback.queue.take().left().getValue());

        // read subscriptions again
        subManager.readSubscriptions(topic, subsCallback, null);
        res = subsCallback.queue.take();
        Assert.assertEquals("Should find just 1 subscriber", 1, res.left().size());
        Assert.assertEquals("Should not fail with PubSubException", null, res.right());
        versionedSubData = res.left().get(subid);
        Assert.assertEquals(Version.Occurred.CONCURRENTLY, v2.compare(versionedSubData.getVersion()));
        imss = res.left().get(subid).getValue();
        Assert.assertEquals("Found inconsistent subscription state", data, imss);
        Assert.assertEquals("Found inconsistent last consumed seq id", seqId, imss.getState().getMsgId()
                .getLocalComponent());

        // update or replace subscription data with bad version
        if (subManager.isPartialUpdateSupported()) {
            subManager.updateSubscriptionData(topic, subid, data, v1, callback, null);
        } else {
            subManager.replaceSubscriptionData(topic, subid, data, v1, callback, null);
        }
        Assert.assertTrue(callback.queue.take().right() instanceof PubSubException.BadVersionException);

        // delete with bad version
        subManager.deleteSubscriptionData(topic, subid, v1, voidCallback, null);
        Assert.assertTrue(callback.queue.take().right() instanceof PubSubException.BadVersionException);
        subManager.deleteSubscriptionData(topic, subid, res.left().get(subid).getVersion(), voidCallback, null);
        Assert.assertEquals("Fail to delete an existed subscriber", null, callback.queue.take().right());

        // read subscription states again
        subManager.readSubscriptions(topic, subsCallback, null);
        res = subsCallback.queue.take();
        Assert.assertEquals("Found more than 0 subscribers", 0, res.left().size());
        Assert.assertEquals("Should not fail with PubSubException", null, res.right());
    }
}
