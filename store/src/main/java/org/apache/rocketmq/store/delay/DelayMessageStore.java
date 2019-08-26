package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
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
        msgQueue.putMessage(req);
        return null;
    }

    public MessageExtBrokerInner getMessage(int queueId, long queueOffset, int size) {
        return new MessageExtBrokerInner();
    }

    public void loadDelayMessageFromStoreToTimingWheel(long startMs, DelayMessageQueue.LoadDelayMessageCallback callback) throws Exception {
        String queueName = simpleDateFormat.format(new Date(startMs));
        DelayMessageQueue msgQueue = delayMessageQueueTable.get(queueName);
        if (msgQueue != null) {
            msgQueue.loadDelayMessageFromStore(0, callback);
        } else {
            log.info("loadDelayMessageFromStoreToTimingWheel DelayMessageQueue is null queueName:{}", queueName);
            throw new Exception("DelayMessageQueue is null queueName:" + queueName);
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


    public void shutdown() {
        // TODO shutdown
    }

}
