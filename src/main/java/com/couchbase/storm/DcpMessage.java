package com.couchbase.storm;

/**
 * Created by davido on 1/23/18.
 */

public class DcpMessage {
    private final DcpMessageType type;
    private String key;
    private String content ;
    private short partition;
    private long startSeqno;
    private long endSeqno;

    public DcpMessage(DcpMessageType type) {
        this.type = type;
    }
    public DcpMessage(DcpMessageType type, String key) {
        this(type);
        this.key = key;
    }
    public DcpMessage(DcpMessageType type, String key, String content) {
        this(type, key);
        this.content = content;
    }
    public DcpMessage(DcpMessageType type, short partition, long startSeqno, long endSeqno) {
        this(type);
        this.partition = partition;
        this.startSeqno = startSeqno;
        this.endSeqno = endSeqno;
    }

    public DcpMessageType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getContent() {
        return content;
    }

    public short getPartition() {
        return partition;
    }

    public long getStartSeqno() {
        return startSeqno;
    }

    public long getEndSeqno() {
        return endSeqno;
    }
}
