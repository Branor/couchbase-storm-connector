package com.couchbase.storm;

/**
 * Created by davido on 1/23/18.
 */
public enum DcpMessageType {
    MUTATION,
    DELETION,
    EXPIRATION,
    SNAPSHOT
}
