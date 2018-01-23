/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.storm;

import com.couchbase.client.dcp.*;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author Sergey Avseyev
 * @author David Ostrovsky
 */
public class DcpConsumer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DcpConsumer.class);
    private final Client client;
    private final String couchbaseBucket;
    private final String couchbasePassword;
    private final List<String> couchbaseNodes;
    private final Consumer<DcpMessage> handler;
    private final int logicalPartitions;
    private final int currentPartition;

    public DcpConsumer(final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword, Consumer<DcpMessage> handler) {
        this(couchbaseNodes, couchbaseBucket, couchbasePassword, handler, 1, 0);
    }
    public DcpConsumer(final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword, Consumer<DcpMessage> handler, int logicalPartitions, int currentPartition) {
        this.couchbaseBucket = couchbaseBucket;
        this.couchbasePassword = couchbasePassword;
        this.couchbaseNodes = couchbaseNodes;
        this.handler = handler;
        this.logicalPartitions = logicalPartitions;
        this.currentPartition = currentPartition;

        client = Client.configure()
                .hostnames(couchbaseNodes)
                .bucket(couchbaseBucket)
                .password(couchbasePassword)
                .build();

        // Don't do anything with control events in this example
        client.controlEventHandler((flowController, event) -> {
            if (DcpSnapshotMarkerRequest.is(event)) {
                handler.accept(new DcpMessage(
                        DcpMessageType.SNAPSHOT,
                        DcpSnapshotMarkerRequest.partition(event),
                        DcpSnapshotMarkerRequest.startSeqno(event),
                        DcpSnapshotMarkerRequest.endSeqno(event)));
                //System.out.println("Snapshot: " + DcpSnapshotMarkerRequest.toString(event));
            }
            event.release();
        });

        // Print out Mutations and Deletions
        client.dataEventHandler((flowController, event) -> {
            if (DcpMutationMessage.is(event)) {
                handler.accept(new DcpMessage(
                        DcpMessageType.MUTATION,
                        DcpMutationMessage.key(event).toString(CharsetUtil.UTF_8),
                        DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8)));
                //System.out.println("Mutation: " + DcpMutationMessage.toString(event));
            } else if (DcpDeletionMessage.is(event)) {
                handler.accept(new DcpMessage(
                        DcpMessageType.DELETION,
                        DcpDeletionMessage.key(event).toString(CharsetUtil.UTF_8)));
                //System.out.println("Deletion: " + DcpDeletionMessage.toString(event));

            }
            else if (DcpExpirationMessage.is(event)) {
                handler.accept(new DcpMessage(
                        DcpMessageType.DELETION,
                        DcpExpirationMessage.key(event).toString(CharsetUtil.UTF_8)));
                //System.out.println("Expiration: " + DcpExpirationMessage.toString(event));
            }
            event.release();
        });
    }

    @Override
    public String toString() {
        return "CouchbaseDcpConsumer{" + "couchbaseBucket=" + couchbaseBucket + '}';
    }

    @Override
    public void run() {
        // Connect the sockets
        client.connect().await();

        // Initialize the state (start now, never stop)
        client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).await();

        // Start streaming on all partitions
        client.startStreaming().await();
    }
}
