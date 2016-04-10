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
import com.couchbase.client.core.message.dcp.RemoveMessage;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A logging DCP handler for remove messages
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
public class ExpirationHandler implements IHandler {

    private static final Logger LOG = Logger.getLogger(ExpirationHandler.class.getName());
    private int count = 0;
    
    @Override
    public void handle(DCPRequest dcp) {
     
        if (dcp instanceof RemoveMessage) {
            
            this.count++;
            
            RemoveMessage msg = (RemoveMessage) dcp;
            
            LOG.log(Level.INFO, "Removed document with bucket/vBucket/key: {0}/{1}/{2}", new Object[]{msg.bucket(), msg.partition(), msg.key()});
            LOG.log(Level.INFO, "So far {0} documents were deleted.", this.count);
        }
    } 
    
}
