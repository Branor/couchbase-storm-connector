package main.java;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by davido on 3/2/15.
 */

public class DcpTypeFilterBolt extends BaseBasicBolt {

    private final List<Class> accepteTypes;
    public DcpTypeFilterBolt() {
        this.accepteTypes = new ArrayList<>();
    }

    public DcpTypeFilterBolt(Class acceptType) {
        this(Arrays.asList(acceptType));
    }

    public DcpTypeFilterBolt(List<Class> acceptTypes) {
        this.accepteTypes = acceptTypes;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("PrinterBolt preparing");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Class type = (Class)tuple.getValueByField("type");
        if(accepteTypes.contains(type))
            collector.emit(tuple.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "key", "content"));
    }

    @Override
    public void cleanup() {
        System.out.println("PrinterBolt cleanup");
    }
}