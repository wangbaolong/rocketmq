package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;

public class DelayMessageManager {

    private TimingWheel timingWheel;
    private DelayMessageStore delayMessageStore;
    private DefaultMessageStore defaultMessageStore;

    public DelayMessageManager(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        timingWheel = new TimingWheel(1000,
                5400,
                System.currentTimeMillis(),
                new DefaultReputExpiredMessageCallback());
        delayMessageStore = new DelayMessageStore();
    }

    public boolean putDelayMessage(DispatchRequest req) {
        DelayMessageStoreResult result = delayMessageStore.putDelayMessage(req);
        DelayMessageInner delayMessage = new DelayMessageInner(result.getTopic(),
                result.getQueueId() * 1000,
                result.getQueueffset(),
                result.getSize());
        timingWheel.add(delayMessage);
        return true;
    }

    private class DefaultReputExpiredMessageCallback implements TimingWheel.ReputExpiredMessageCallback {

        @Override
        public void callback(DelayMessageInner msg) {
            MessageExtBrokerInner msgInner = delayMessageStore.getMessage((int) (msg.getExpirationMs() / 1000), msg.getQueueffset(), msg.getSize());
            PutMessageResult result = defaultMessageStore.getCommitLog().putMessage(msgInner);
            if (result.isOk()) {
                delayMessageStore.setDispatchLog();
            } else {
                // TODO 重试10次 如果不成功则添加到死信队列
            }
        }
    }

}
