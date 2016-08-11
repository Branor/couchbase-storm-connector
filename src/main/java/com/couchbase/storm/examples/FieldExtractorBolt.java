package com.couchbase.storm.examples;
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

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.couchbase.client.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by David Ostrovsky on 3/2/15.
 */

public class FieldExtractorBolt extends BaseBasicBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("FieldExtractorBolt ready");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String content = (String)tuple.getValueByField("content");
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> jsonMap = mapper.readValue(content, new TypeReference<Map<String, Object>>() {});

                collector.emit(new Values(
                        tuple.getValueByField("type"),
                        tuple.getValueByField("key"),
                        jsonMap.getOrDefault("user", null),
                        jsonMap.getOrDefault("text", null),
                        jsonMap.getOrDefault("date", null),
                        jsonMap.getOrDefault("sentiment", null),
                        jsonMap.getOrDefault("sentimentScore", null)));

        } catch (IOException e) {

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "key", "user", "text", "date", "sentiment", "sentimentScore"));
    }

    @Override
    public void cleanup() {
        System.out.println("PrinterBolt cleanup");
    }
}