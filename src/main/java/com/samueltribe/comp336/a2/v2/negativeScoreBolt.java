package com.samueltribe.comp336.a2.v2;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

public class negativeScoreBolt extends BaseRichBolt {

    private Set<String> NEGATIVE_LIST = new HashSet<>(Arrays.asList(
            "bad", "awful", "terrible", "ineffective", "unacceptable", "atrocious",
            "inferior", "garbage", "rubbish", "inadequate", "danger", "dangerous", "untested",
            "rushed", "muzzled", "muzzle", "plandemic", "conspiracy", "lying", "evil","lies","lie",
            "fake"
    ));

    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = (String) tuple.getValueByField("word");

        int negativeNum = 0;
        if (NEGATIVE_LIST.contains(word.toLowerCase())) {
            negativeNum++;
        }

        _collector.emit(new Values(negativeNum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("negativeNum"));
    }
}