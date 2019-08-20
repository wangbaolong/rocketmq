package org.apache.rocketmq.store.delay;

public interface ReputExpiredMessageCallback {
    void callback(DelayMessageInner msg);
}
