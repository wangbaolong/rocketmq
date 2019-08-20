package org.apache.rocketmq.test.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.delay.DelayMessageInner;
import org.apache.rocketmq.store.delay.ReputExpiredMessageCallback;
import org.apache.rocketmq.store.delay.TimingWheel;

public class TimingWheelTest {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    public static void main(String[] args) {
        long currentTime = System.currentTimeMillis();
        TimingWheel timingWheel = new TimingWheel(1000,
                5400,
                null,
                new ReputExpiredMessageCallback(){
                    @Override
                    public void callback(DelayMessageInner msg) {
                        log.info("ReputExpiredMessageCallback callback expirationMs:" + msg.getExpirationMs() + "  currentMillTime:" + System.currentTimeMillis());
                    }
                });
        long delay = currentTime + 0;
        DelayMessageInner msg = new DelayMessageInner("TopicTest", delay, 1000, 64);
        timingWheel.add(msg);
        delay = currentTime + 8000;
        msg = new DelayMessageInner("TopicTest", delay, 1000, 64);
        timingWheel.add(msg);
        delay = currentTime + 8000;
        msg = new DelayMessageInner("TopicTest", delay, 1000, 64);
        timingWheel.add(msg);
        delay = currentTime + 10000;
        msg = new DelayMessageInner("TopicTest", delay, 1000, 64);
        timingWheel.add(msg);
        delay = currentTime + 12000;
        msg = new DelayMessageInner("TopicTest", delay, 1000, 64);
        timingWheel.add(msg);
        delay = currentTime + 15000;
        msg = new DelayMessageInner("TopicTest", delay, 1000, 64);
        timingWheel.add(msg);
    }
}
