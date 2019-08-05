package org.apache.rocketmq.store.delay;

public class DelayMessageStoreResult {

    private int status;
    private String topic;
    private int queueId;
    private long queueffset;
    private int size;

    public DelayMessageStoreResult(String topic, int queueId, long queueffset, int size) {
        this.topic = topic;
        this.queueId = queueId;
        this.queueffset = queueffset;
        this.size = size;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getQueueffset() {
        return queueffset;
    }

    public int getSize() {
        return size;
    }

    public int getStatus() {
        return status;
    }
}
