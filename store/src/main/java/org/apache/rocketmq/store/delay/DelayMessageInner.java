package org.apache.rocketmq.store.delay;

public class DelayMessageInner {

    private String topic;
    private long expirationMs;
    private long queueffset;
    private int size;

    DelayMessageInner next;
    DelayMessageInner prev;

    public DelayMessageInner(String topic, long expirationMs, long queueffset, int size) {
        this.topic = topic;
        this.expirationMs = expirationMs;
        this.queueffset = queueffset;
        this.size = size;
    }


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getExpirationMs() {
        return expirationMs;
    }

    public void setExpirationMs(long expirationMs) {
        this.expirationMs = expirationMs;
    }

    public long getQueueffset() {
        return queueffset;
    }

    public void setQueueffset(long queueffset) {
        this.queueffset = queueffset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
