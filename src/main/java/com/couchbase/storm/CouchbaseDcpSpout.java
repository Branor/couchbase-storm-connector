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

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("serial")
public class CouchbaseDcpSpout extends BaseRichSpout {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseDcpSpout.class);
    private final String couchbaseBucket;
    private final String couchbasePassword;
    private final List<String> couchbaseNodes;

    SpoutOutputCollector _collector;
    LinkedBlockingQueue<DcpMessage> queue = null;

    public CouchbaseDcpSpout(final String couchbaseNodes, final String couchbaseBucket, final String couchbasePassword) {
        this(splitNodes(couchbaseNodes), couchbaseBucket, couchbasePassword);
    }

    public CouchbaseDcpSpout(final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword) {
        this.couchbaseBucket = couchbaseBucket;
        this.couchbasePassword = couchbasePassword;
        this.couchbaseNodes = couchbaseNodes;
    }

    private static List<String> splitNodes(final String nodes) {
        return Arrays.asList(nodes.split(","));
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<>(1000);
        _collector = collector;

        int spoutInstances = context.getComponentTasks(context.getThisComponentId()).size();
        int currentIndex = context.getThisTaskIndex();
        LOGGER.debug("Couchbase DCP Spout instance: " + currentIndex + " out of total: " + spoutInstances);

        DcpConsumer dcp = new DcpConsumer(couchbaseNodes, couchbaseBucket, couchbasePassword, queue::offer, spoutInstances, currentIndex);
        dcp.run();
    }


    @Override
    public void nextTuple() {
        String key = "";
        String content = "";

        DcpMessage message = queue.poll();
        if (message == null) {
            Utils.sleep(50);
        } else {
            switch(message.getType()) {
                case MUTATION:
                    key = message.getKey();
                    content = message.getContent();
                    break;
                case DELETION:
                    key = message.getKey();
                    break;
                case EXPIRATION:
                    key = message.getKey();
                    break;
                case SNAPSHOT:
                    key = "snapshot";
                    content = "vBucket: " + message.getPartition() + ", start sequence: " + message.getStartSeqno() + ", end sequence: " + message.getEndSeqno();
                    break;
            }
            _collector.emit(new Values(message.getType(), key, content));
        }
    }

    @Override
    public void close() {
        //TODO: close DCP connection
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "key", "content"));
    }

}