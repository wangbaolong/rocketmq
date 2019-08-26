package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class DelayMessageDispatchStore {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;

    private volatile long delayMsgDispatchTimestamp = 0;

    public DelayMessageDispatchStore(final String scpPath) throws IOException {
        File file = new File(scpPath);
        MappedFile.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MappedFile.OS_PAGE_SIZE);

        if (fileExists) {
            log.info("store delay message dispatch file exists, " + scpPath);
            delayMsgDispatchTimestamp = mappedByteBuffer.getLong();
        }


    }

    public void setDelayMsgDispatchTimestamp(long timestamp) {
        this.delayMsgDispatchTimestamp = timestamp;
    }

    public long getDelayMsgDispatchTimestamp() {
        return delayMsgDispatchTimestamp;
    }

    public void shutdown() {
       this.flush();
        MappedFile.clean(this.mappedByteBuffer);
        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Failed to properly close the channel", e);
        }
    }

    public void flush() {
        this.mappedByteBuffer.putLong(0, this.delayMsgDispatchTimestamp);
        this.mappedByteBuffer.force();
    }
}
