package com.samueltribe.comp336.a2.v1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

public class negativeBolt extends BaseRichBolt {

    // This is the list of negative words to match in the tweet
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
        // The words in the tweet are retrieved
        ArrayList<String> words = (ArrayList<String>) tuple.getValueByField("words");


        int negativeNum = 0;

        for (String word : words) {
            if (NEGATIVE_LIST.contains(word.toLowerCase())) {
                // The number of negative words in the tweet are counted
                negativeNum++;
            }
        }
        /* The total number of negative and positive words are emitted along with the words in the tweet (this was for
         * debugging purposes
         */
        _collector.emit(new Values(words,tuple.getValueByField("positiveNum"),negativeNum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("words","positiveNum","negativeNum"));
    }
}