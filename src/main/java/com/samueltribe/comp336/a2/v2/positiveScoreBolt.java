package com.samueltribe.comp336.a2.v2;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

public class positiveScoreBolt extends BaseRichBolt {

    private Set<String> POSITIVE_LIST = new HashSet<>(Arrays.asList(
            "good","great","progress","excellent","favourable","exceptional",
            "superb","wonderful","effective","nice","excitement","excited",
            "hope","hopeful","protection","safe","happy","successful","grateful",
            "thankful","humbled"
    ));

    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = (String) tuple.getValueByField("word");

        int positiveNum = 0;

        if (POSITIVE_LIST.contains(word.toLowerCase())) {
            positiveNum++;
        }

        _collector.emit(new Values(positiveNum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("positiveNum"));
    }
}
