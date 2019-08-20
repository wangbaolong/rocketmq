package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
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
            if (body != null) {
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

    public List<MappedFile> getMappedFiles() {
        return this.mappedFileQueue.getMappedFiles();
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public long getCanReadPosition() {
        MappedFile last = mappedFileQueue.getLastMappedFile();
        if (last != null) {
            return last.getFileFromOffset() + last.getReadPosition();
        }
        return 0;
    }

    public void loadDelayMessageFromStore(long startOffset, LoadDelayMessageCallback callback) {
        // Cocurrent load delay message
        startOffset = loadDelayMessageFromStore0(startOffset, callback);
        try {
            // Synchronous load delay message
            lock.lock();
            loadDelayMessageFromStore0(startOffset, callback);
        }  finally {
            lock.unlock();
        }

    }

    private long loadDelayMessageFromStore0(long startOffset,  LoadDelayMessageCallback callback) {
        long canReadPosition = getCanReadPosition();
        if (startOffset >= canReadPosition) {
            log.info("DelayMessageQueue queueName:{} loadDelayMessageFromStoreToTimingWheel canReadPosition = 0", queueName);
            return startOffset;
        }
        for (boolean doNext = true; doNext && startOffset < canReadPosition;) {
            MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(startOffset);
            if (mappedFile != null) {
                int pos = (int) (startOffset % mappedFileSize);
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
                if (result != null) {
                    for (int readSize = 0; readSize < result.getSize(); ) {
                        DelayMessageInner msgInner = readDelayMessage(result.getByteBuffer(), startOffset);
                        if (msgInner != null && msgInner.getSize() > 0) {
                            startOffset += msgInner.getSize();
                            if (callback != null) {
                                callback.callback(msgInner);
                            }
                        } else {
                            readSize += result.getSize();
                        }
                    }
                    startOffset += result.getSize();
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

            byte[] bytesContent = new byte[totalSize];
            // 2 MAGIC CODE
//            int magicCode = byteBuffer.getInt();
//            int bodyCRC = byteBuffer.getInt();

            byteBuffer.position(byteBuffer.position()
                    + 4 // magicCode
                    + 4 // bodyCRC
            );

            int queueId = byteBuffer.getInt();


//            int flag = byteBuffer.getInt();
//
//            long queueOffset = byteBuffer.getLong();
//
//            long physicOffset = byteBuffer.getLong();
//
//            int sysFlag = byteBuffer.getInt();
//
//            long bornTimeStamp = byteBuffer.getLong();
//
//            ByteBuffer byteBuffer1 = byteBuffer.get(bytesContent, 0, 8);
//
//            ByteBuffer byteBuffer2 = byteBuffer.get(bytesContent, 0, 8);
//
//            int reconsumeTimes = byteBuffer.getInt();
//
//            long preparedTransactionOffset = byteBuffer.getLong();

            byteBuffer.position(byteBuffer.position()
                    + 4 // flag
                    + 8 // queueOffset
                    + 8 // physicOffset
                    + 4 // sysFlag
                    + 8 // bornTimeStamp
                    + 8 // bornHost
                    + 8 // storeHost
                    + 4 // reconsumeTimes
                    + 8 // preparedTransactionOffset
            );

            int bodyLen = byteBuffer.getInt();
            byteBuffer.position(byteBuffer.position()
                    + bodyLen
            );
//            if (bodyLen > 0) {
//                byteBuffer.get(bytesContent, 0, bodyLen);
//            }

            byte topicLen = byteBuffer.get();
//            byteBuffer.get(bytesContent, 0, topicLen);
//            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            byteBuffer.position(byteBuffer.position()
                    + topicLen
            );

            short propertiesLength = byteBuffer.getShort();
            byteBuffer.position(byteBuffer.position()
                    + propertiesLength
            );
            return new DelayMessageInner(queueId * 1000, startOffset, totalSize);
        } catch (Exception e) {

        }
        return null;
    }

    public interface LoadDelayMessageCallback {
        void callback(DelayMessageInner msgInner);
    }

}
