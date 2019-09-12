package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class DelayMessageQueue {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    private final MappedFileQueue mappedFileQueue;
    private final String queueName;
    private final String storePath;
    private final int mappedFileSize;
    private final ByteBuffer byteBuffer;
    private final ReentrantLock lock;
    private final int maxDelayMessageSize;
    private long maxPhysicOffset = -1;

    public DelayMessageQueue(final String queueName,
                             final String storePath,
                             final int mappedFileSize,
                             final DefaultMessageStore defaultMessageStore) {
        this.queueName = queueName;
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.maxDelayMessageSize = defaultMessageStore.getMessageStoreConfig().getMaxDelayMessageSize();
        this.byteBuffer = ByteBuffer.allocate(this.maxDelayMessageSize);
        this.lock = new ReentrantLock();

        String queueDir = this.storePath
                + File.separator + queueName;
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
    }

    public DelayMessageStoreResult putMessage(DelayMessageDispatchRequest req) {
        if (req.getMsgSize() + req.getCommitLogOffset() <= maxPhysicOffset) {
            log.warn("Maybe try to build delay queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, req.getCommitLogOffset());
            return new DelayMessageStoreResult(false);
        }
        try {
            this.lock.lock();
            byteBuffer.flip();
            byteBuffer.limit(this.maxDelayMessageSize);
            byteBuffer.putInt(req.getMsgSize());
            byteBuffer.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            byteBuffer.putInt(req.getBodyCRC());
            byteBuffer.putInt(req.getQueueId());
            byteBuffer.putInt(req.getFlag());
            byteBuffer.putLong(req.getConsumeQueueOffset());
            byteBuffer.putLong(req.getCommitLogOffset());
            byteBuffer.putInt(req.getSysFlag());
            byteBuffer.putLong(req.getBornTimestamp());
            byteBuffer.put(req.getBornHost());
            byteBuffer.putLong(req.getStoreTimestamp());
            byteBuffer.put(req.getStoreHost());
            byteBuffer.putInt(req.getReconsumeTimes());
            byteBuffer.putLong(req.getPreparedTransactionOffset());

            byte[] body = req.getBody();
            if (body != null && body.length > 0) {
                byteBuffer.putInt(body.length);
                byteBuffer.put(body);
            } else {
                byteBuffer.putInt(0);
            }

            byteBuffer.put((byte) req.getTopic().length());
            byteBuffer.put(req.getTopic().getBytes(MessageDecoder.CHARSET_UTF8));

            String propertiesString = MessageDecoder.messageProperties2String(req.getPropertiesMap());
            if (propertiesString != null && propertiesString.length() > 0) {
                byte[] properties = propertiesString.getBytes(MessageDecoder.CHARSET_UTF8);
                byteBuffer.putShort((short) properties.length);
                byteBuffer.put(properties);
            } else {
                byteBuffer.putShort((short) 0);
            }
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
            if (mappedFile == null || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0);
            }

            if (mappedFile == null) {
                log.error("create mapped file1 error, topic: " + req.getTopic() + " storeTimestamp: " + req.getStoreTimestamp());
                return new DelayMessageStoreResult(false);
            }
            long queueffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
            boolean result = mappedFile.appendMessage(byteBuffer.array(), 0, req.getMsgSize());
            if (!result) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                if (mappedFile == null) {
                    log.error("create mapped file2 error, topic: " + req.getTopic() + " storeTimestamp: " + req.getStoreTimestamp());
                    return new DelayMessageStoreResult(false);
                }
                queueffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
                result = mappedFile.appendMessage(byteBuffer.array(), 0, req.getMsgSize());
            }
            if (result) {
                this.maxPhysicOffset = req.getCommitLogOffset() + req.getMsgSize();
                return new DelayMessageStoreResult(req.getQueueId(), queueffset, req.getMsgSize());
            } else {
                return new DelayMessageStoreResult(false);
            }
        }catch(Exception e) {
            log.error("DelayMessageQueue putMessage error :{}", e.getMessage());
        } finally {
            this.lock.unlock();
        }
        return new DelayMessageStoreResult(false);
    }

    public long getCanReadPosition() {
        MappedFile last = mappedFileQueue.getLastMappedFile();
        if (last != null) {
            return last.getFileFromOffset() + last.getReadPosition();
        }
        return 0;
    }

    public void loadMessageFromStore(long startOffset, LoadMessageCallback callback) {
        // Cocurrent load delay message
        startOffset = loadMessageFromStoreInternal(startOffset, callback);
        try {
            // Synchronous load delay message
            lock.lock();
            loadMessageFromStoreInternal(startOffset, callback);
        }  finally {
            lock.unlock();
        }
    }

    public void loadExpiredMessage(long startOffset, LoadMessageCallback callback) {
        loadMessageFromStoreInternal(startOffset, callback);
    }

    private long loadMessageFromStoreInternal(long startOffset, LoadMessageCallback callback) {
        long canReadPosition = getCanReadPosition();
        if (startOffset >= canReadPosition) {
            log.info("DelayMessageQueue queueName:{} loadMessageFromStoreToTimingWheel canReadPosition = 0", queueName);
            return startOffset;
        }
        for (boolean doNext = true; doNext && startOffset < canReadPosition;) {
            MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(startOffset);
            if (mappedFile != null) {
                int pos = (int) (startOffset % mappedFileSize);
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
                if (result != null) {
                    try {
                        for (int readSize = 0; readSize < result.getSize(); ) {
                            DelayMessageInner msgInner = readDelayMessage(result.getByteBuffer(), startOffset);
                            if (msgInner != null && msgInner.getSize() > 0) {
                                readSize += msgInner.getSize();
                                if (callback != null) {
                                    callback.callback(msgInner);
                                }
                            } else {
                                readSize = result.getSize();
                            }
                        }
                        startOffset += result.getSize();
                    } finally {
                        result.release();
                    }
                } else {
                    doNext = false;
                }
            } else {
                doNext = false;
            }
        }
        return startOffset;
    }

    private DelayMessageInner readDelayMessage(ByteBuffer byteBuffer, long startOffset) {
        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            if (totalSize <= 0) {
                return new DelayMessageInner(0);
            }

            byteBuffer.position(byteBuffer.position()
                    + 4 // magicCode
                    + 4 // bodyCRC
            );

            int queueId = byteBuffer.getInt();

            byteBuffer.position(byteBuffer.position()
                    + 4 // flag
                    + 8 // queueOffset
                    + 8 // physicOffset
                    + 4 // sysFlag
                    + 8 // bornTimeStamp
                    + 8 // bornHost
                    + 8 // storeTimestamp
                    + 8 // storeHost
                    + 4 // reconsumeTimes
                    + 8 // preparedTransactionOffset
            );

            int bodyLen = byteBuffer.getInt();
            byteBuffer.position(byteBuffer.position()
                    + bodyLen
            );
            byte topicLen = byteBuffer.get();
            byteBuffer.position(byteBuffer.position()
                    + topicLen
            );

            short propertiesLength = byteBuffer.getShort();
            byteBuffer.position(byteBuffer.position()
                    + propertiesLength
            );
            return new DelayMessageInner(queueId * 1000L, startOffset, totalSize);
        } catch (Exception e) {
            log.error("Load delay message  ");
        }
        return null;
    }

    public MessageExtBrokerInner getMessage(long queueOffset, int size) {
        int mappedFileSize = this.mappedFileSize;
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(queueOffset, queueOffset == 0);
        if (mappedFile != null) {
            int pos = (int) (queueOffset % mappedFileSize);
            SelectMappedBufferResult result =  mappedFile.selectMappedBuffer(pos, size);
            if (result != null) {
                try {
                    MessageExt messageExt = MessageDecoder.decode(result.getByteBuffer(), true, false);
                    return this.messageTimeup(messageExt);
                } finally {
                    result.release();
                }
            }
        }
        return null;
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load delay queue {} result :", this.queueName, result);
        return result;
    }

    public long recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }
            int mappedFileSize = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while(true) {
                while (true) {
                    int msgSize = byteBuffer.getInt();// message size
                    if (msgSize <= 0) {
                        break;
                    }
                    byteBuffer.position(byteBuffer.position()
                            + 4 // magicCode
                            + 4 // bodyCRC
                            + 4 // queueId
                            + 4 // flag
                            + 8 // queueOffset
                    );
                    long physicOffset = byteBuffer.getLong();
                    byteBuffer.position(byteBuffer.position()
                            + 4 // sysFlag
                            + 8 // bornTimeStamp
                            + 8 // bornHost
                            + 8 // storeTimestamp
                            + 8 // storeHost
                            + 4 // reconsumeTimes
                            + 8 // preparedTransactionOffset
                    );
                    int bodyLen = byteBuffer.getInt();
                    byteBuffer.position(byteBuffer.position()
                            + bodyLen
                    );
                    byte topicLen = byteBuffer.get();
                    byteBuffer.position(byteBuffer.position()
                            + topicLen
                    );
                    short propertiesLength = byteBuffer.getShort();
                    byteBuffer.position(byteBuffer.position()
                            + propertiesLength
                    );
                    this.maxPhysicOffset = physicOffset + msgSize;
                    mappedFileOffset += msgSize;
                }
                index++;
                if (index >= mappedFiles.size()) {
                    log.info("recover last delay queue file over, last mapped file "
                            + mappedFile.getFileName());
                    break;
                } else {
                    mappedFile = mappedFiles.get(index);
                    byteBuffer = mappedFile.sliceByteBuffer();
                    processOffset = mappedFile.getFileFromOffset();
                    mappedFileOffset = 0;
                    log.info("recover next delay queue file, " + mappedFile.getFileName());
                }
            }
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
        return this.maxPhysicOffset;
    }

    public boolean flush(final int flushLeastPages) {
        return this.mappedFileQueue.flush(flushLeastPages);
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);

        msgInner.setSysFlag(MessageSysFlag.cleanDelayMessage(msgExt.getSysFlag()));
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);

        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
//        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_DATE_TIME);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        return msgInner;
    }

    public interface LoadMessageCallback {
        void callback(DelayMessageInner msgInner);
    }

}
