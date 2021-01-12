package com.samueltribe.comp336.a2.v2;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

    static final String TOPOLOGY_NAME = "storm-twitter-comp336-a2";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);

        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterCovidSpout", new Spout());
        b.setBolt("filterBolt", new filterBolt(8)).shuffleGrouping("TwitterCovidSpout");
        b.setBolt("positiveScoreBolt", new positiveScoreBolt()).allGrouping("filterBolt");
        b.setBolt("negativeScoreBolt", new negativeScoreBolt()).allGrouping("filterBolt");
        b.setBolt("totalScoresBolt", new TotalScoresBolt(5)).shuffleGrouping("positiveScoreBolt").shuffleGrouping("negativeScoreBolt");

        config.setMaxTaskParallelism(1);

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });
    }

}
