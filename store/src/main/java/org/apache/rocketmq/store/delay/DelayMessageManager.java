package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class DelayMessageManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    private TimingWheel timingWheel;
    private DelayMessageStore delayMessageStore;
    private DefaultMessageStore defaultMessageStore;
    private DelayMessageDispatchStore delayMessageDispatchStore;

    public DelayMessageManager(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public boolean putDelayMessage(DelayMessageDispatchRequest req) {
        DelayMessageStoreResult result = delayMessageStore.putMessage(req);
        DelayMessageInner delayMessage = new DelayMessageInner(
                result.getQueueId() * 1000,
                result.getQueueffset(),
                result.getSize());
        timingWheel.add(delayMessage);
        return true;
    }

    public boolean load(boolean lastExitOK) {
        boolean result = false;
        String dispatchPath = StorePathConfigHelper.getStorePathDelayMessageDispatch(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        try {
            this.delayMessageDispatchStore = new DelayMessageDispatchStore(dispatchPath);
            result = true;
        } catch (IOException e) {
            e.printStackTrace();

        }
        this.delayMessageStore = new DelayMessageStore(defaultMessageStore);
        result = result && this.delayMessageStore.load();
        return result;
    }

    public void start() {
        timingWheel = new TimingWheel(1000,
                5400,
                delayMessageStore,
                new DefaultReputExpiredMessageCallback());
    }

    private class DefaultReputExpiredMessageCallback implements ReputExpiredMessageCallback {

        @Override
        public void callback(DelayMessageInner msg) {
            MessageExtBrokerInner msgInner = delayMessageStore.getMessage((int) (msg.getExpirationMs() / 1000), msg.getQueueffset(), msg.getSize());
            PutMessageResult result = defaultMessageStore.getCommitLog().putMessage(msgInner);
            if (result.isOk()) {
                delayMessageDispatchStore.setDelayMsgDispatchTimestamp(msg.getExpirationMs());
            } else {
                // TODO 重试10次 如果不成功则添加到死信队列
            }
        }
    }

    public ConcurrentHashMap<String, DelayMessageQueue> getDelayMessageQueueTable() {
        return this.delayMessageStore.getDelayMessageQueueTable();
    }

    public void shutdown() {
        // TODO shutdown
        delayMessageStore.shutdown();
        delayMessageDispatchStore.shutdown();
        timingWheel.shutdown();
    }

}
