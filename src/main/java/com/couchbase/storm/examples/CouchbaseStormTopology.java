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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.storm.CouchbaseDcpSpout;
import com.couchbase.storm.DcpTypeFilterBolt;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import java.io.IOException;


/**
 * Created by David Ostrovsky on 3/2/15.
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
                .withPath("/couchbase/");

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