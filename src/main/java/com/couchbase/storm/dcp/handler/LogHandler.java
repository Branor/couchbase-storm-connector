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

package com.couchbase.storm.dcp.handler;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.storm.dcp.Receiver;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A logging DCP handler
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
public class LogHandler implements IHandler {

    private static final Logger LOG = Logger.getLogger(Receiver.class.getName());
    
    @Override
    public void handle(DCPRequest dcp) {
         
        LOG.log(Level.INFO, "Message is {0} on bucket/vBucket: {1}/{2}", new Object[]{dcp.getClass(), dcp.bucket(), dcp.partition()});        
   
        if (dcp instanceof MutationMessage)
        {
            MutationMessage msg = (MutationMessage)dcp;
            LOG.log(Level.INFO, "Mutation: key = {0}, cas = {1}, ttl = {2}", new Object[]{msg.key(), msg.cas(), msg.expiration()});
        }
        
        
        if (dcp instanceof SnapshotMarkerMessage)
        {
            SnapshotMarkerMessage msg = (SnapshotMarkerMessage)dcp;
            LOG.log(Level.INFO, "Snapshot: vBucket = {0}, start = {1}, end = {2}", new Object[]{msg.partition(), msg.startSequenceNumber(), msg.endSequenceNumber()});
        }
        
        if (dcp instanceof RemoveMessage) {
           
            RemoveMessage msg = (RemoveMessage)dcp;
            LOG.log(Level.INFO, "Remove: key = {0}", msg.key());
        }
    }
}
