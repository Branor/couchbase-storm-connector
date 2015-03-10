package com.couchbase.storm;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Created by David Ostrovsky on 3/2/15.
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