package org.apache.rocketmq.store.delay;

public class DelayMessageInner {

    private long expirationMs;
    private long queueffset;
    private int size;

    DelayMessageInner next;
    DelayMessageInner prev;

    public DelayMessageInner(long expirationMs, long queueffset, int size) {
        this.expirationMs = expirationMs;
        this.queueffset = queueffset;
        this.size = size;
    }

    public DelayMessageInner(int size) {
        this.size = size;
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
