# couchbase-storm-connector
Connector for consuming a Couchbase DCP stream as a Storm spout.

Example usage:

    Config conf = new Config();
    LocalCluster cluster = new LocalCluster();
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("couchbase", new CouchbaseDcpSpout("localhost", "default"), 1);
    cluster.submitTopology("test", conf, builder.createTopology());

Note that the CouchbaseDcpSpout currently only supports a parallelism of 1. Load balancing DCP streams across vBuckets will be implemented eventually.
