/*
 * Copyright 2015 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.storm.dcp;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.message.dcp.StreamRequestResponse;
import rx.Observable;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;


/**
 * This is the receiver of the DCP stream
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
public class Receiver {
    
    /**
     * A logger
     */
    private static final Logger LOG = Logger.getLogger(Receiver.class.getName());
    
    
    /**
     * The connection timeout to the cluster in seconds
     */
    public static final int CONNECT_TIMEOUT = 2;
    
    /**
     * The name of the DCP stream
     */
    public static final String STREAM_NAME = "dcp-receiver";
    
    /**
     * At first a Core environment is required
     */
    private final ReceiverEnv env;
    
    /**
     * The core of the Couchbase client. DCP is not yet completely exposed, 
     * so we are using a core component here which is used by the Couchbase
     * client itself.
     */
    private final ClusterFacade core;
    
    
    /**
     * The nodes to connect to during the bootstrap phase
     */
    private final List<String> nodes;
    
    
    /**
     * The bucket to connect to
     */
    private final String bucket;
    
    /**
     * The bucket password
     */
    private final String password;
    
    /**
     * The connection state
     */
    private boolean connected = false;
    
    /**
     * To handle the DCP result
     */
    private Consumer<DCPRequest> handler;
    
    /**
     * Initialize the receiver
     * @param nodes
     * @param bucket
     * @param password
     */
    public Receiver(List<String> nodes, String bucket, String password, Consumer<DCPRequest> handler) {
                
        this(nodes, bucket, password, handler, new ReceiverEnv());

    }
    
    /**
     * Initialize the receiver by passing an environment
     * 
     * @param nodes
     * @param bucket
     * @param env 
     * @param password
     */
    public Receiver(List<String> nodes, String bucket, String password, Consumer<DCPRequest> handler, ReceiverEnv env) {
        
        this.env = env;
        this.core = new CouchbaseCore(env);
        this.nodes = nodes;
        this.bucket = bucket;
        this.password = password;
        this.handler = handler;
        
    }
    
    /**
     * Since the Receiver is initialized we want to connect to the Couchbase Cluster
     */
    public void connect()
    {
        //This sets up the bootstrap nodes for 
        core.send(new SeedNodesRequest(nodes))
                .timeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
                .toBlocking()
                .single();
        
        //Now open a bucket connection
        core.send(new OpenBucketRequest(bucket, password))
                .timeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
                .toBlocking()
                .single();
        
        connected = true;
    }
    
    /**
     * Open the DCP streams and handle them by using the passed handler
     * 
     */
    public void stream()
    {
        core.send(new OpenConnectionRequest(STREAM_NAME, bucket))
                .toList()
                .flatMap(resp -> numOfVBuckets()) //Send a cluster config request and map the result to the number of vBuckets
                .flatMap(count -> requestStreams(count)) //Stream by taking the number of vBuckets into account
                .toBlocking()
                .forEach(dcp -> this.handler.accept(dcp)); //Now handle every result of the stream here
    }
    
    /**
     * Retrieve the number of vBuckets of the Bucket
     * @return 
     */
    private Observable<Integer> numOfVBuckets()
    {
        return core.<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .map(cfg -> { 
                     
                    CouchbaseBucketConfig bucketCfg = (CouchbaseBucketConfig) cfg.config().bucketConfig(bucket);
                    return bucketCfg.numberOfPartitions();
                            
                } );
    }
    
    /**
     *  Request the streams for all vBuckets
     * 
     * @return 
     */
    private Observable<DCPRequest> requestStreams(int numOfVBuckets)
    {
        return Observable.merge( //Merge the streams to one stream
                Observable.range(0, numOfVBuckets) //For each vBucket
                .flatMap(vBucket -> core.<StreamRequestResponse>send(new StreamRequestRequest(vBucket.shortValue(), bucket))) //Request a stream
                .map(response -> response.stream()) //Return the stream as Observable of DCPRequest
        );               
    }  
}
