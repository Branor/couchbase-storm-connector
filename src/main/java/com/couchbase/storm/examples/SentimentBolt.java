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

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.couchbase.client.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by David Ostrovsky on 3/2/15.
 */

public class SentimentBolt extends BaseBasicBolt {

    private static final Map<Integer, String> scores;

    static {
        Map<Integer, String> map = new HashMap<>();
        map.put(0, "Very Negative");
        map.put(1, "Negative");
        map.put(2, "Neutral");
        map.put(3, "Positive");
        map.put(4, "Very Positive");
        scores = Collections.unmodifiableMap(map);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("SentimentBolt preparing");

        NLP.init();
        NLP.findSentiment("hello");

        System.out.println("SentimentBolt ready");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String content = (String)tuple.getValueByField("content");
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> jsonMap = mapper.readValue(content, new TypeReference<Map<String, Object>>() {});
            if(!jsonMap.containsKey("sentiment") || !jsonMap.containsKey("sentimentScore")) {
                String tweet = (String)jsonMap.getOrDefault("text", "");
                int sentiment = NLP.findSentiment(tweet);
                jsonMap.put("sentimentScore", sentiment);
                jsonMap.put("sentiment", scores.get(sentiment));
                String json = mapper.writeValueAsString(jsonMap);


                collector.emit(new Values(tuple.getValueByField("type"),
                                          tuple.getValueByField("key"),
                                          json));
            }

        } catch (IOException e) {

        }
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