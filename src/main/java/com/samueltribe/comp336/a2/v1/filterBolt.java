package com.samueltribe.comp336.a2.v1;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;
import java.util.*;
public class filterBolt extends BaseRichBolt {
    // Irrelevant words are filtered out of the tweet text, any of the following words are removed (inspired from example code given)
    private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList("http", "https", "the", "you", "que", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get",
            "about", "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "para", "por",
            "want", "will", "know", "from", "las", "don", "people", "got", "why", "con", "time", "would"));
    OutputCollector _collector;
    public filterBolt() { }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }
    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        // Punctuation and new lines are dealt with using regex here, based off example code given
        String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        // The tweet is split into an ArrayList of strings (words)
        ArrayList<String> words = new ArrayList<String>(Arrays.asList(text.split(" ")));
        ArrayList<String> filteredWords = new ArrayList<String>();
        for (int i = 0; i < words.size(); i++) {
            if (!IGNORE_LIST.contains(words.get(i))) {
                // If the current word is not in the ignore list then it is added to the new filteredWords ArrayList
                filteredWords.add(words.get(i));
            }
        }
        // Once all words have been filtered, emit the new tweet text as an ArrayList
        _collector.emit(new Values(filteredWords));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("filteredWords"));
    }
}