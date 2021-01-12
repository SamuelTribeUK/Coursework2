package com.samueltribe.comp336.a2.v1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

import java.util.*;

public class positiveBolt extends BaseRichBolt {

    // This is the list of positive words to match in the tweet
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
        // The words in the tweet are retrieved
        ArrayList<String> words = (ArrayList<String>) tuple.getValueByField("filteredWords");

        int positiveNum = 0;

        for (String word : words) {
            if (POSITIVE_LIST.contains(word.toLowerCase())) {
                // The number of positive words in the tweet is counted
                positiveNum++;
            }
        }
        // The total number of positive words is emitted along with the tweet text
        _collector.emit(new Values(words,positiveNum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("words","positiveNum"));
    }
}