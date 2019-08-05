package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TimingWheel {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    private long tickMs;
    private int wheelSize;
    private long interval;
    private AtomicInteger msgCounter;
    private long currentTime;
    private long startMs;
    private TimingWheelBucket[] buckets;
    private ExecutorService bossExecutor;
    private ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    private Lock readLock = reentrantReadWriteLock.readLock();
    private Lock writeLock = reentrantReadWriteLock.writeLock();
    private ReputExpiredMessageCallback reputExpiredMessageCallback;
    private OverflowTimingWheel overflowTimingWheel;

    public TimingWheel(long tickMs, int wheelSize, long startMs, ReputExpiredMessageCallback callback) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.interval = tickMs * wheelSize;
        this.reputExpiredMessageCallback = callback;

        this.msgCounter = new AtomicInteger(0);
        this.currentTime = startMs - (startMs % tickMs);
        this.startMs = startMs;
        this.buckets = new TimingWheelBucket[wheelSize];
        for (int i = 0; i < wheelSize; i++) {
            this.buckets[i] = new TimingWheelBucket();
        }
        overflowTimingWheel = new OverflowTimingWheel(interval, wheelSize, startMs);
        initBossExeutor();
    }

    private void initBossExeutor() {
        bossExecutor = new ThreadPoolExecutor(2,
                2,
                60 , TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new TimingWheelThreadFactory(),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        log.error("TimingWheel Thread pool rejected");
                    }
                });

        bossExecutor.submit((Runnable) () -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    advanceClock();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    public void add(DelayMessageInner msg) {
        try {
            readLock.lock();
            if (msg.getExpirationMs() < currentTime + tickMs) {
                doReputAsync(msg);
            } else if (msg.getExpirationMs() < currentTime + interval) {
                addInner(msg);
            } else {

            }
        } finally {
            readLock.unlock();
        }
    }

    private void addInner(DelayMessageInner msg) {
        long virtualId = (msg.getExpirationMs() - currentTime) / tickMs;
        int index = (int) virtualId % wheelSize - 1;
        TimingWheelBucket bucket = buckets[index];
        bucket.addDelayMessage(msg);
        msgCounter.incrementAndGet();

        // TODO 两个bucket 一个作为缓存
    }

    public void advanceClock() {
        try {
            writeLock.lock();
            currentTime += tickMs;
            int virtualId = (int) ((currentTime - startMs) / tickMs);
            int index = virtualId % wheelSize;
            TimingWheelBucket bucket = buckets[index];
            Iterator<DelayMessageInner> it = bucket.getDelayMessageListAndResetBucket();
            log.info("this.currentTime:{}", this.currentTime);
            doReputBatchAsync(it);
            // TODO 检查时间轮剩余消息，达到一定阈值则从磁盘加载新消息
        } finally {
            writeLock.unlock();
        }
    }

    private void doReputBatchAsync(Iterator<DelayMessageInner> it) {
        bossExecutor.submit(new Runnable() {
            @Override
            public void run() {
                while(it.hasNext()) {
                    reputDelayMessageCallback(it.next());
                }
            }
        });
    }

    private void doReputAsync(DelayMessageInner msg) {
        bossExecutor.submit(new Runnable() {
            @Override
            public void run() {
                reputDelayMessageCallback(msg);
            }
        });
    }

    private void reputDelayMessageCallback(DelayMessageInner msg) {
        msgCounter.decrementAndGet();
        reputExpiredMessageCallback.callback(msg);
    }

    public interface ReputExpiredMessageCallback {
        void callback(DelayMessageInner msg);
    }

    private static class TimingWheelThreadFactory implements ThreadFactory {

        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        TimingWheelThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = "TimingWheel-" +
                    poolNumber.getAndIncrement() +
                    "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    public int getMessageCount() {
        return msgCounter.get();
    }

}
