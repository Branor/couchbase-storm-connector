package main.java;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.*;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import rx.Observable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by davido on 3/2/15.
 */
public class CouchbaseStormTopology {
    public static void main(String[] args) throws IOException {
        String nodes = args[0];
        String bucket = args[1];

        TopologyBuilder builder = new TopologyBuilder();

        System.setProperty("com.couchbase.dcpEnabled", "true");

        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/foo/");

        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://hdp:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        builder.setSpout("couchbase", new CouchbaseDcpSpout(nodes, bucket), 1);
        builder.setBolt("filter", new DcpTypeFilterBolt(MutationMessage.class), 2).shuffleGrouping("couchbase");
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("filter");
        builder.setBolt("hdfs", hdfsBolt, 1).shuffleGrouping("filter");


        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());

        //Utils.sleep(100000);
        System.in.read();
        //cluster.shutdown();
    }
}
