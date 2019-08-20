package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;

public class DelayMessageManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    private TimingWheel timingWheel;
    private DelayMessageStore delayMessageStore;
    private LoadMessageManager loadMessageManager;
    private DefaultMessageStore defaultMessageStore;

    public DelayMessageManager(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.delayMessageStore = new DelayMessageStore(defaultMessageStore);
        this.loadMessageManager = new LoadMessageManager(delayMessageStore);
        timingWheel = new TimingWheel(1000,
                5400,
                loadMessageManager,
                new DefaultReputExpiredMessageCallback());
    }

    public boolean putDelayMessage(DelayMessageDispatchRequest req) {
        delayMessageStore.putMessage(req);
        DelayMessageStoreResult result = delayMessageStore.putMessage(req);
        DelayMessageInner delayMessage = new DelayMessageInner(
                result.getQueueId() * 1000,
                result.getQueueffset(),
                result.getSize());
        timingWheel.add(delayMessage);
        return true;
    }

    private class DefaultReputExpiredMessageCallback implements ReputExpiredMessageCallback {

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
