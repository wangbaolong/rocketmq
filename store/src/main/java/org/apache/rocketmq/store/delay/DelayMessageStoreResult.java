package org.apache.rocketmq.store.delay;

public class DelayMessageStoreResult {

    private boolean success;
    private int queueId;
    private long queueffset;
    private int size;

    public DelayMessageStoreResult(int queueId, long queueffset, int size) {
        this.queueId = queueId;
        this.queueffset = queueffset;
        this.size = size;
        this.success = false;
    }

    public DelayMessageStoreResult(boolean success) {
        this.success = success;
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

    public boolean getStatus() {
        return success;
    }
}
