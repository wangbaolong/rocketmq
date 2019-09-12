package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DelayMessageStore {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    private ConcurrentHashMap<String, DelayMessageQueue> delayMessageQueueTable;
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
    private DefaultMessageStore defaultMessageStore;

    public DelayMessageStore(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        delayMessageQueueTable = new ConcurrentHashMap<String, DelayMessageQueue>(32);
    }

    public DelayMessageStoreResult putMessage(DelayMessageDispatchRequest req) {
        String queueName = simpleDateFormat.format(new Date(req.getTimeUnit().toMillis(req.getQueueId())));
        DelayMessageQueue msgQueue = delayMessageQueueTable.get(queueName);
        if (msgQueue == null) {
            String storePath = StorePathConfigHelper.getStorePathDelayMessageQueue(this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
            int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getDelayMessageQueueMappedFileSize();
            msgQueue = new DelayMessageQueue(queueName, storePath, mappedFileSize, defaultMessageStore);
            delayMessageQueueTable.put(queueName, msgQueue);
        }
        return msgQueue.putMessage(req);
    }

    public MessageExtBrokerInner getMessage(int queueId, long queueOffset, int size) {
        String queueName = simpleDateFormat.format(new Date(queueId * 1000L));
        DelayMessageQueue msgQueue = delayMessageQueueTable.get(queueName);
        if (msgQueue == null) {
            log.info("DelayMessageStore getMessage msgQueue is null {} {} {}", queueId, queueOffset, size);
            return null;
        }
        return msgQueue.getMessage(queueOffset, size);
    }

    public void loadMessageFromStoreToTimingWheel(long startMs, DelayMessageQueue.LoadMessageCallback callback) throws Exception {
        String queueName = simpleDateFormat.format(new Date(startMs));
        DelayMessageQueue msgQueue = delayMessageQueueTable.get(queueName);
        if (msgQueue != null) {
            msgQueue.loadMessageFromStore(0, callback);
        } else {
            log.info("loadMessageFromStoreToTimingWheel DelayMessageQueue is null queueName:{}", queueName);
        }
    }

    public void loadExpiredMessage(long startTimestamp, long endTimestamp,
                                   DelayMessageQueue.LoadMessageCallback callback) throws ParseException {
        String startQueueName = simpleDateFormat.format(new Date(startTimestamp));
        String endQueueName = simpleDateFormat.format(new Date(endTimestamp));

        long startDateTime = simpleDateFormat.parse(startQueueName).getTime();
        long endDateTime = simpleDateFormat.parse(endQueueName).getTime();

        long hourTimeMillis = 60 * 60 * 1000;

        for( ; startDateTime <= endDateTime; startDateTime += hourTimeMillis) {
            startQueueName = simpleDateFormat.format(new Date(startDateTime));
            DelayMessageQueue msgQueue = delayMessageQueueTable.get(startQueueName);
            if (msgQueue != null) {
                msgQueue.loadExpiredMessage(0, callback);
            }
        }
    }

    public boolean  load() {
        String storePath = StorePathConfigHelper.getStorePathDelayMessageQueue(this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        File dirDelay = new File(storePath);
        File[] fileQueueList = dirDelay.listFiles();
        if (fileQueueList != null) {
            for (File fileQueue : fileQueueList) {
                String queueName = fileQueue.getName();
                int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getDelayMessageQueueMappedFileSize();
                DelayMessageQueue msgQueue = new DelayMessageQueue(queueName, storePath, mappedFileSize, defaultMessageStore);
                if (!msgQueue.load()) {
                    return false;
                }
                delayMessageQueueTable.put(queueName, msgQueue);
            }
        }
        log.info("load delay queue all over, OK");
        return true;
    }

    public ConcurrentHashMap<String, DelayMessageQueue> getDelayMessageQueueTable() {
        return delayMessageQueueTable;
    }

    public long recover() {
        long maxPhysicalOffset = -1;
        for (Map.Entry<String, DelayMessageQueue> entry : delayMessageQueueTable.entrySet()) {
            maxPhysicalOffset = Math.max(entry.getValue().recover(), maxPhysicalOffset);
        }
        return maxPhysicalOffset;
    }


    public void shutdown() {
        // TODO shutdown
    }

}
