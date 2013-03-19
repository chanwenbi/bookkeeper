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
package org.apache.hedwig.server.persistence;

import java.io.File;
import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.topics.TrivialOwnAllTopicManager;
import org.apache.hedwig.util.FileUtils;

public class TestLeveldbPersistenceManagerBlackBox extends TestPersistenceManagerBlackBox {

    protected ServerConfiguration conf;
    protected File leveldbDir;

    @Override
    protected void setUp() throws Exception {
        leveldbDir = FileUtils.createTempDirectory("hub-persistencedb", "test");
        conf = new ServerConfiguration() {
            @Override
            public String getLeveldbPersistencePath() {
                try {
                    return leveldbDir.getCanonicalPath().toString();
                } catch (IOException e) {
                    return null;
                }
            }
        };
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (null != leveldbDir) {
            org.apache.commons.io.FileUtils.deleteDirectory(leveldbDir);
        }
    }

    @Override
    long getLowestSeqId() {
        return 1;
    }

    @Override
    PersistenceManager instantiatePersistenceManager() throws Exception {
        OrderedSafeExecutor scheduler = new OrderedSafeExecutor(conf.getNumTopicQueuerThreads());
        return new LeveldbPersistenceManager(conf, new TrivialOwnAllTopicManager(conf, scheduler), scheduler);
    }

    @Override
    long getExpectedSeqId(int numPublished) {
        return numPublished;
    }

    public static Test suite() {
        return new TestSuite(TestLeveldbPersistenceManagerBlackBox.class);
    }

}
