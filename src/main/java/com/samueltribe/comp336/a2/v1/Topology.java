package com.samueltribe.comp336.a2.v1;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
public class Topology {
    static final String TOPOLOGY_NAME = "storm-twitter-comp336-a2";
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterCovidSpout", new Spout());
        b.setBolt("filterBolt", new filterBolt()).shuffleGrouping("TwitterCovidSpout");
        b.setBolt("positiveBolt", new positiveBolt()).shuffleGrouping("filterBolt");
        b.setBolt("negativeBolt", new negativeBolt()).shuffleGrouping("positiveBolt");
        b.setBolt("scoreBolt", new scoreBolt(60)).shuffleGrouping("negativeBolt");
        // Max Task Parallelism set to 1 for running on VPS, this can be set higher
        config.setMaxTaskParallelism(1);
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());
        // After just over 24 hours the cluster will be killed and shutdown
        Thread.sleep(86405000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}