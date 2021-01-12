package com.samueltribe.comp336.a2.v2;

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

public class filterBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(filterBolt.class);

    private final long logIntervalSec;

    private long tweetCount = 0;
    private long removeCount = 0;
    private long lastLogTime;

    private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList("http", "https", "the", "you", "que", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get",
            "about", "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "para", "por",
            "want", "will", "know", "from", "las", "don", "people", "got", "why", "con", "time", "would"));

    OutputCollector _collector;

    public filterBolt(long logIntervalSec) {
        this.logIntervalSec = logIntervalSec;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        lastLogTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        tweetCount++;

        String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        ArrayList<String> words = new ArrayList<String>(Arrays.asList(text.split(" ")));

        for (int i = 0; i < words.size(); i++) {
            if (!IGNORE_LIST.contains(words.get(i))) {
                _collector.emit(new Values(words.get(i)));
            } else {
                removeCount++;
            }
        }

        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {
            logger.info("\n");
            logger.info("Tweet count: " + tweetCount);
            logger.info("Recent Tweet: " + tweet.getText());
            logger.info("\n");

            lastLogTime = now;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
