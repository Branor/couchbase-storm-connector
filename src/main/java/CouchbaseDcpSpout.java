package main.java;
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.message.dcp.*;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class CouchbaseDcpSpout extends BaseRichSpout {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseDcpSpout.class);
    private transient DcpConsumerEnvironment env;
    private final String couchbaseBucket;
    private final List<String> couchbaseNodes;

    SpoutOutputCollector _collector;
    LinkedBlockingQueue<DCPRequest> queue = null;
    String nodes;


    public CouchbaseDcpSpout(final String couchbaseNodes, final String couchbaseBucket) {
        this(splitNodes(couchbaseNodes), couchbaseBucket);
    }

    public CouchbaseDcpSpout(final List<String> couchbaseNodes, final String couchbaseBucket) {
        this.couchbaseBucket = couchbaseBucket;
        this.couchbaseNodes = couchbaseNodes;
    }

    private static List<String> splitNodes(final String nodes) {
        return Arrays.asList(nodes.split(","));
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<DCPRequest>(1000);
        _collector = collector;
        this.env = DefaultDcpConsumerEnvironment.create();

        DcpConsumer dcp = new DcpConsumer(couchbaseNodes, couchbaseBucket, env, message -> {
            queue.offer(message);
        });
        dcp.run();
    }


    @Override
    public void nextTuple() {
        String key = "";
        String content = "";

        DCPRequest request = queue.poll();
        if (request == null) {
            Utils.sleep(50);
        } else {
            if (request instanceof MutationMessage) {
                MutationMessage message = (MutationMessage) request;
                key = message.key();
                content = message.content().toString(CharsetUtil.UTF_8);
            // handle document change/update
            } else if (request instanceof RemoveMessage) {
                key = ((RemoveMessage) request).key();
                // handle document deletion
            } else if(request instanceof  SnapshotMarkerMessage) {
                SnapshotMarkerMessage message = (SnapshotMarkerMessage) request;
                key = "snapshot";
                content = "vBucket: " + message.partition() + ", start sequence: " + message.startSequenceNumber() + ", end sequence: " + message.endSequenceNumber();
            }

            _collector.emit(new Values(request.getClass(), key, content));
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