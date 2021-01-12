package com.samueltribe.comp336.a2.v1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

public class scoreBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(scoreBolt.class);

    private final long logIntervalSec;

    private long lastLogTime;
    private long lastWriteTime;
    private double tweetTotal;

    private double totalPositiveTweets;
    private double totalNegativeTweets;
    private double totalNeutralTweets;

    private long totalPositivePoints;
    private long totalNegativePoints;

    // The write variables below are for writing to the csv file
    private int writeTweetNum;
    private int writePositiveTweetNum;
    private int writeNeutralTweetNum;
    private int writeNegativeTweetNum;

    OutputCollector _collector;

    String initLine[] = {"date","time","number of tweets","positive tweets","neutral tweets","negative tweets"};

    public scoreBolt(long logIntervalSec) {
        this.logIntervalSec = logIntervalSec;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        lastLogTime = System.currentTimeMillis();
        lastWriteTime = System.currentTimeMillis();
        try {
            CSVWriter writer = new CSVWriter(new FileWriter("output.csv"));
            writer.writeNext(initLine);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        // The words from the tweet and the positive and negative scores are retrieved
        ArrayList<String> words = (ArrayList<String>) tuple.getValueByField("words");
        int positiveNum = (int) tuple.getValueByField("positiveNum");
        int negativeNum = (int) tuple.getValueByField("negativeNum");

        tweetTotal++;
        writeTweetNum++;

        totalNegativePoints += negativeNum;
        totalPositivePoints += positiveNum;

        // The overall score is calculated and it is determined if the tweet is positive, negative or neutral
        int score = positiveNum - negativeNum;

        if (score > 0) {
            totalPositiveTweets++;
            writePositiveTweetNum++;
        } else if (score == 0) {
            totalNeutralTweets++;
            writeNeutralTweetNum++;
        } else {
            totalNegativeTweets++;
            writeNegativeTweetNum++;
        }

        /* For any future implementations I have emitted the words and total score here, possibly for a separate csv
         * writing bolt as mentioned in my report
         */
        _collector.emit(new Values(words,score));

        long now = System.currentTimeMillis();

        // Below is logging code
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {

            DecimalFormat nf = new DecimalFormat("0.###");

            logger.info("\n\n");
            logger.info("Number of Tweets: " + tweetTotal);
            logger.info("Number of Positive Tweets: " + totalPositiveTweets);
            logger.info("Number of Neutral Tweets: " + totalNeutralTweets);
            logger.info("Number of Negative Tweets: " + totalNegativeTweets);
            logger.info("-------------------------------");
            logger.info("Total Positive Points: " + totalPositivePoints);
            logger.info("Total Negative Points: " + totalNegativePoints);
            logger.info("-------------------------------");
            logger.info("Positive Tweet %: " + nf.format((totalPositiveTweets / tweetTotal) * 100) + "%");
            logger.info("Neutral Tweet %: " + nf.format((totalNeutralTweets / tweetTotal) * 100) + "%");
            logger.info("Negative Tweet %: " + nf.format((totalNegativeTweets / tweetTotal) * 100) + "%");


            lastLogTime = now;
        }

        // Below is writing to csv code
        long writePeriodSec = (now - lastWriteTime) / 1000;
        // Currently write to the csv file every 10 minutes
        long writeIntervalSec = 600;
        SimpleDateFormat datef = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat timef = new SimpleDateFormat("HH:mm");
        Date date = new Date();
        if (writePeriodSec > writeIntervalSec) {
            CSVWriter writer = null;
            try {
                writer = new CSVWriter(new FileWriter("output.csv",true));
            } catch (IOException e) {
                e.printStackTrace();
            }
            // Write a new result to csv
            String[] newResult = {datef.format(date),
                    timef.format(date),
                    Integer.toString(writeTweetNum),
                    Integer.toString(writePositiveTweetNum),
                    Integer.toString(writeNeutralTweetNum),
                    Integer.toString(writeNegativeTweetNum)};
            writer.writeNext(newResult);
            try {
                writer.flush();
                logger.info("Results written to file successfully");

            } catch (IOException e) {
                logger.info("Error in writing results to file!");
                logger.info(e.getMessage());
            }

            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            lastWriteTime = now;
            writeTweetNum = 0;
            writePositiveTweetNum = 0;
            writeNeutralTweetNum = 0;
            writeNegativeTweetNum = 0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("words","score"));
    }
}