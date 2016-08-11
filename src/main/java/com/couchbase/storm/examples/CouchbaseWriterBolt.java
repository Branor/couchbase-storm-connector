package com.couchbase.storm.examples;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import java.util.Map;

public class CouchbaseWriterBolt extends BaseBasicBolt {

    private String nodes;
    private String bucketName;
    private Bucket bucket;
    private Cluster cluster;

    public CouchbaseWriterBolt(final String nodes, final String bucketName) {
        this.nodes = nodes;
        this.bucketName = bucketName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("CouchbaseWriterBolt preparing");

        DefaultCouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().maxRequestLifetime(75000).build();
        cluster = CouchbaseCluster.create(env, nodes);
        bucket = cluster.openBucket(bucketName);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String key = (String)tuple.getValueByField("key");
        String content = (String)tuple.getValueByField("content");
        bucket.upsert(RawJsonDocument.create(key, content));
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