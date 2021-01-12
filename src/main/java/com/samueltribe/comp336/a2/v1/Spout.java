package com.samueltribe.comp336.a2.v1;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
public class Spout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // The tweet queue can hold 2500 tweets, however this is never reached with these hashtag filters
        queue = new LinkedBlockingQueue<Status>(2500);
        this.collector = spoutOutputCollector;
        StatusListener sl = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            @Override
            public void onTrackLimitationNotice(int i) {}
            @Override
            public void onScrubGeo(long l, long l1) {}
            @Override
            public void onStallWarning(StallWarning stallWarning) {}
            @Override
            public void onException(Exception e) {}
        };
        // Using a FilterQuery to filter the incoming Tweet stream for the following hashtags
        FilterQuery fq = new FilterQuery();
        String[] keywords = {"#COVIDVaccine", "#covidvaccine", "#CovidVaccine", "#vaccine","#Vaccine", "#COVID19Vaccine","#covid19vaccine"};
        fq.track(keywords);
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(sl);
        twitterStream.filter(fq);
    }
    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if (ret == null) {
            // If the queue is empty then wait 250ms, this is set high for VPS purposes
            Utils.sleep(250);
        } else {
            // The next Tweet from the queue is emitted for use by the filter bolt
            collector.emit(new Values(ret));
        }
    }
    @Override
    public void close() { twitterStream.shutdown(); }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }
    @Override
    public void ack(Object id) {}
    @Override
    public void fail(Object id) {}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
}
