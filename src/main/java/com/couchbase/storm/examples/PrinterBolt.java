package com.couchbase.storm.examples;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by davido on 3/2/15.
 */

public class PrinterBolt extends BaseBasicBolt {

    private int count = 0;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("PrinterBolt preparing");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
//        System.out.println(tuple);

        count++;


        if(count % 1 == 0)
            System.out.print('.');
        if(count % 100 == 0)
            System.out.println(count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        System.out.println("PrinterBolt declareOutputFields");
    }

    @Override
    public void cleanup() {
        System.out.println("PrinterBolt cleanup");
    }
}