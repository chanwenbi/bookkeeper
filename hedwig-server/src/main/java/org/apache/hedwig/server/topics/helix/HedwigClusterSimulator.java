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
package org.apache.hedwig.server.topics.helix;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Simulator a hedwig cluster to verify Helix logics.
 */
public class HedwigClusterSimulator {

    private static final int START_PORT = 12000;
    private static int NUM_NODES = 3;
    private static final int NUM_PARTITIONS = 10;
    private static final int NUM_REPLICAS = 2;

    private static List<InstanceConfig> INSTANCE_CONFIG_LIST;
    private static List<HelixTopicManager> TOPICMANAGER_LIST;
    private static HelixAdmin admin;
    private static final ServerConfiguration conf = new ServerConfiguration();

    static {
        INSTANCE_CONFIG_LIST = new ArrayList<InstanceConfig>();
        TOPICMANAGER_LIST = new ArrayList<HelixTopicManager>();
        for (int i = 0; i < NUM_NODES; i++) {
            INSTANCE_CONFIG_LIST.add(createInstanceConfig(START_PORT + i));
        }
    }

    static InstanceConfig createInstanceConfig(int port) {
        InstanceConfig instanceConfig = new InstanceConfig("localhost_" + port);
        instanceConfig.setHostName("localhost");
        instanceConfig.setPort("" + port);
        instanceConfig.setInstanceEnabled(true);
        return instanceConfig;
    }

    static void echo(String info) {
        System.out.println(info);
    }

    static void setupCluster() {
        // admin to initialize the cluster.
        admin = new ZKHelixAdmin(conf.getZkHost());
        echo("Creating cluster : " + HelixTopicManager.DEFAULT_CLUSTERNAME);
        admin.addCluster(HelixTopicManager.DEFAULT_CLUSTERNAME, true);

        // Add nodes to the cluster.
        echo("Adding " + NUM_NODES + " participants to the cluster.");
        for (int i = 0; i < NUM_NODES; i++) {
            admin.addInstance(HelixTopicManager.DEFAULT_CLUSTERNAME, INSTANCE_CONFIG_LIST.get(i));
            echo("\t Added participant: " + INSTANCE_CONFIG_LIST.get(i).getInstanceName());
        }

        // Add a state model
        StateModelDefinition stateModel = States.defineTopicStateModel();
        echo("Configuring cluster : " + HelixTopicManager.DEFAULT_CLUSTERNAME + " with 1 Leader and 2 Standby.");
        admin.addStateModelDef(HelixTopicManager.DEFAULT_CLUSTERNAME, States.STATE_MODEL_NAME, stateModel);
        // for now we just have only one topic namespace, in future we could adapt to more namespace
        echo("Adding a namespace " + HelixTopicManager.DEFAULT_NAMESPACE + " with " + NUM_PARTITIONS
                + " partitions, each partition hosted by " + NUM_REPLICAS + " replicas.");
        admin.addResource(HelixTopicManager.DEFAULT_CLUSTERNAME, HelixTopicManager.DEFAULT_NAMESPACE, NUM_PARTITIONS,
                States.STATE_MODEL_NAME, "AUTO");
        // initialize the cluster
        // it calculates the preference list for each partition.
        admin.rebalance(HelixTopicManager.DEFAULT_CLUSTERNAME, HelixTopicManager.DEFAULT_NAMESPACE, NUM_REPLICAS);
    }

    static void startController() {
        // start controller
        echo("Starting Helix Controller");
        HelixControllerMain.startHelixController(conf.getZkHost(), HelixTopicManager.DEFAULT_CLUSTERNAME,
                "localhost_9100", HelixControllerMain.STANDALONE);
    }

    static void startCluster() throws Exception {
        echo("Starting participants in the cluster.");
        for (int i = 0; i < NUM_NODES; i++) {
            InstanceConfig instanceConf = INSTANCE_CONFIG_LIST.get(i);
            /**
            HelixTopicManager tm = new HelixTopicManager(conf, instanceConf);
            TOPICMANAGER_LIST.add(tm);
            tm.start();
            echo("\t Started Topic Manager : " + instanceConf.getInstanceName());
            **/
        }
    }

    static void startZooKeeper() throws Exception {
        echo("Starting ZooKeeper at " + conf.getZkHost());
        File zkTmpDir = File.createTempFile("zookeeper", "test");
        if (!zkTmpDir.delete() || !zkTmpDir.mkdirs()) {
            throw new IOException("Couldn't create zk directory " + zkTmpDir);
        }
        ZooKeeperServer zks;
        zks = new ZooKeeperServer(zkTmpDir, zkTmpDir, 2181);
        NIOServerCnxnFactory serverFactory = new NIOServerCnxnFactory();
        serverFactory.configure(new InetSocketAddress(2181), 100);
        serverFactory.startup(zks);
    }

    static void addNode() throws Exception {
        NUM_NODES = NUM_NODES + 1;
        int port = START_PORT + NUM_NODES - 1;
        InstanceConfig instanceConfig = createInstanceConfig(port);
        echo("Adding New Node : " + instanceConfig.getInstanceName()
                + ". Partitions will move from old nodes to the new node.");
        admin.addInstance(HelixTopicManager.DEFAULT_CLUSTERNAME, instanceConfig);
        INSTANCE_CONFIG_LIST.add(instanceConfig);
        /**
        HelixTopicManager tm = new HelixTopicManager(conf, instanceConfig);
        TOPICMANAGER_LIST.add(tm);
        admin.rebalance(HelixTopicManager.DEFAULT_CLUSTERNAME, HelixTopicManager.DEFAULT_NAMESPACE, NUM_REPLICAS);
        tm.start();
        **/
    }

    static void rebalance() {
        admin.rebalance(HelixTopicManager.DEFAULT_CLUSTERNAME, HelixTopicManager.DEFAULT_NAMESPACE, NUM_REPLICAS);
    }

    static void restartNode(InstanceConfig config) throws Exception {
        echo("Restarting " + config.getInstanceName() + ". Mastership will be transferred back.");
        /**
        HelixTopicManager tm = new HelixTopicManager(conf, config);
        TOPICMANAGER_LIST.set(Integer.parseInt(config.getPort()) - START_PORT, tm);
        tm.start();
        **/
    }

    static InstanceConfig stopNode() throws Exception {
        Random r = new Random(System.currentTimeMillis());
        int nodeId = r.nextInt(NUM_NODES - 1);
        echo("Stopping " + INSTANCE_CONFIG_LIST.get(nodeId).getInstanceName()
                + ". Mastership will be transferred to the remaining nodes.");
        HelixTopicManager tm = TOPICMANAGER_LIST.get(nodeId);
        tm.stop();
        return tm.getInstanceConfig();
    }

    static void printState(String msg) {
        echo("CLUSTER STATE: " + msg);
        ExternalView resourceExternalView = admin.getResourceExternalView(HelixTopicManager.DEFAULT_CLUSTERNAME,
                HelixTopicManager.DEFAULT_NAMESPACE);
        if (null == resourceExternalView) {
            return;
        }
        TreeSet<String> sortedSet = new TreeSet<String>(resourceExternalView.getPartitionSet());
        StringBuilder sb = new StringBuilder("\t\t");
        for (int i = 0; i < NUM_NODES; i++) {
            sb.append(INSTANCE_CONFIG_LIST.get(i).getInstanceName()).append("\t");
        }
        echo(sb.toString());
        for (String partitionName : sortedSet) {
            sb.delete(0, sb.length() - 1);
            sb.append(partitionName).append("\t");
            for (int i = 0; i < NUM_NODES; i++) {
                Map<String, String> stateMap = resourceExternalView.getStateMap(partitionName);
                if (null != stateMap && stateMap.containsKey(INSTANCE_CONFIG_LIST.get(i).getInstanceName())) {
                    sb.append(stateMap.get(INSTANCE_CONFIG_LIST.get(i).getInstanceName()).charAt(0)).append("\t\t");
                } else {
                    sb.append("-").append("\t\t");
                }
            }
            System.out.println(sb);
        }
        System.out.println("####################################################");
    }

    public static void main(String[] args) throws Exception {
        startZooKeeper();
        setupCluster();
        // printState("Initialized Cluster.");
        startCluster();
        startController();
        Thread.sleep(10000);
        printState("After starting " + NUM_NODES + " nodes.");
        InstanceConfig config = stopNode();
        Thread.sleep(10000);
        printState("After a random node stops/crashes.");
        rebalance();
        Thread.sleep(5000);
        printState("After rebalancing.");
        restartNode(config);
        Thread.sleep(10000);
        printState("Restart the failed node.");
        rebalance();
        Thread.sleep(5000);
        printState("After rebalancing.");
        echo("END!!!");
        System.exit(0);
    }
}
