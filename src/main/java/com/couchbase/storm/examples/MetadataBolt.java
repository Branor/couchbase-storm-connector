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

import com.couchbase.client.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by David Ostrovsky on 3/2/15.
 */

public class MetadataBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String content = (String)tuple.getValueByField("content");
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> jsonMap = mapper.readValue(content, new TypeReference<Map<String, Object>>() {});
            if(true || !jsonMap.containsKey("hashtags") || !jsonMap.containsKey("usernames")) {
                String tweet = (String)jsonMap.getOrDefault("text", "");
                String user = (String)jsonMap.getOrDefault("user", "");

                String[] tokens = tweet.split("[ \\:\\.\\!\\?]");
                List<String> tags = new ArrayList<>();
                List<String> users = new ArrayList<>();
                users.add(user);
                for(String token: tokens)
                {
                    if(token.startsWith("#"))
                        tags.add(token.substring(1));
                    if(token.startsWith("@"))
                        users.add(token.substring(1));
                }

                jsonMap.put("hashtags", tags);
                jsonMap.put("usernames", users);
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