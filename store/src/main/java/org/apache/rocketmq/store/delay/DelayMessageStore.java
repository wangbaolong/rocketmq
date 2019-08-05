package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageExtBrokerInner;

public class DelayMessageStore {



    public void load() {

    }

    public void recover() {

    }

    public void destory() {

    }

    public DelayMessageStoreResult putDelayMessage(DispatchRequest req) {
        return null;
    }

    public MessageExtBrokerInner getMessage(int queueId, long queueOffset, int size) {
        return new MessageExtBrokerInner();
    }

    public void setDispatchLog() {

    }

}
