package com.samueltribe.comp336.a2.v2;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.samueltribe.comp336.a2.v1.scoreBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.Map;

public class TotalScoresBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(scoreBolt.class);

    private final long logIntervalSec;

    private long lastLogTime;

    private long totalPositivePoints = 0;
    private long totalNegativePoints = 0;

    OutputCollector _collector;

    public TotalScoresBolt(long logIntervalSec) {
        this.logIntervalSec = logIntervalSec;
    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        lastLogTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        // If the incoming tupl
        try {
            int positiveScore = (int) tuple.getValueByField("positiveNum");
            totalPositivePoints += positiveScore;
        } catch (IllegalArgumentException e) {

        }

        try {
            int negativeScore = (int) tuple.getValueByField("negativeNum");
            totalNegativePoints += negativeScore;
        } catch (IllegalArgumentException e) {

        }

        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {

            DecimalFormat nf = new DecimalFormat("0.###");

            logger.info("\n\n");
            logger.info("-------------------------------");
            logger.info("Total Positive Points: " + totalPositivePoints);
            logger.info("Total Negative Points: " + totalNegativePoints);
            logger.info("-------------------------------");

            lastLogTime = now;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
