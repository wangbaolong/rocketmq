package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DelayMessageManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    private TimingWheel timingWheel;
    private DelayMessageStore delayMessageStore;
    private DefaultMessageStore defaultMessageStore;
    private DelayMessageDispatchStore delayMessageDispatchStore;
    private FlushDelayQueueService flushDelayQueueService;

    public DelayMessageManager(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.flushDelayQueueService = new FlushDelayQueueService();
    }

    public boolean putDelayMessage(DelayMessageDispatchRequest req) {
        DelayMessageStoreResult result = delayMessageStore.putMessage(req);
        if (result.isSuccess()) {
            DelayMessageInner delayMessage = new DelayMessageInner(
                    result.getQueueId() * 1000L,
                    result.getQueueffset(),
                    result.getSize());
            timingWheel.add(delayMessage);
        } else {
            log.info("DelayMessageManager putDelayMessage failed request:{}", req);
        }
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

    public long recover() {
        long maxPhhysicalOffset = delayMessageStore.recover();
        timingWheel = new TimingWheel(1000,
                3600,
                delayMessageStore,
                new DefaultReputExpiredMessageCallback());
        return maxPhhysicalOffset;
    }

    public void start() {
        flushDelayQueueService.start();
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

    class FlushDelayQueueService extends ServiceThread {

        private static final int RETRY_TIMES_OVER = 3;
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
            int flushConsumeQueueLeastPages = defaultMessageStore.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            int flushConsumeQueueThoroughInterval = defaultMessageStore.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
            }

            ConcurrentHashMap<String, DelayMessageQueue> delayMessageQueueTable = getDelayMessageQueueTable();
            for (Map.Entry<String, DelayMessageQueue> entry : delayMessageQueueTable.entrySet()) {
                boolean result = false;
                for (int i = 0; i < retryTimes && !result; i++) {
                    result = entry.getValue().flush(flushConsumeQueueLeastPages);
                }
            }

        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    int interval = defaultMessageStore.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushDelayQueueService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60;
        }

    }

}
