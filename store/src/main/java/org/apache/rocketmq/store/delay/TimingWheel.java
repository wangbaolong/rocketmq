package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.Calendar;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TimingWheel {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    private long tickMs;
    private int wheelSize;
    private long interval;
    private long currentTime;
    private long startMs;
    private TimingWheelBucket[] buckets;
    private ExecutorService reputExpeiredMessageService;
    private ScheduledExecutorService scheduledExecutorService;
    private ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    private Lock readLock = reentrantReadWriteLock.readLock();
    private Lock writeLock = reentrantReadWriteLock.writeLock();
    private ReputExpiredMessageCallback reputExpiredMessageCallback;
    private int preloadThresholdIndex;
    private LoadMessageManager loadMessageManager;

    public TimingWheel(long tickMs, int wheelSize, DelayMessageStore delayMessageStore, ReputExpiredMessageCallback callback) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.interval = tickMs * wheelSize;
        this.reputExpiredMessageCallback = callback;
        this.preloadThresholdIndex = (int) (wheelSize * (80.0 / 100));
        this.loadMessageManager = new LoadMessageManager(delayMessageStore,
                startMs + interval,
                tickMs,
                wheelSize,
                interval);
        // TODO startMs 调整为整小时，currentTime为当前时间
        this.currentTime = System.currentTimeMillis();
        this.currentTime = currentTime - (currentTime % tickMs);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(currentTime);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        startMs = calendar.getTimeInMillis();
        buckets = loadMessageManager.getCurrentBuckets(currentTime, tickMs, wheelSize, currentTime);
        loadMessageManager.startHandleExpiredMessage(currentTime, callback);
        initExeutorService();
    }

    private void initExeutorService() {
        reputExpeiredMessageService = new ThreadPoolExecutor(1,
                1,
                60 , TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new DelayThreadFactory("reputExpeiredMessageService"));

        scheduledExecutorService = Executors.newScheduledThreadPool(1,
                new DelayThreadFactory("scheduledExecutorService"));

        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                advanceClock();
            }
        }, tickMs, tickMs, TimeUnit.MILLISECONDS);

    }

    public void add(DelayMessageInner msg) {
        try {
            readLock.lock();
            if (msg.getExpirationMs() < currentTime + tickMs) {
                doReputAsync(msg);
            } else if (msg.getExpirationMs() < startMs + interval) {
                addInner(msg);
            } else {
                loadMessageManager.putMessage(msg);
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
            // TODO 两个bucket 一个作为缓存
            if (index + 1 == wheelSize) {
                buckets = loadMessageManager.getPreloadBuckets();
                resetStartMsAndCurrentTime();
            }
            if (index >= preloadThresholdIndex) {
                // TODO 检查时间轮剩余消息，达到一定阈值则启动磁盘数据预加载
                loadMessageManager.startPreloadBuckets();
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void resetStartMsAndCurrentTime() {
        startMs = currentTime;
        loadMessageManager.resetstartMs(startMs + interval);
    }

    private void doReputBatchAsync(Iterator<DelayMessageInner> it) {
        reputExpeiredMessageService.submit(new Runnable() {
            @Override
            public void run() {
                while(it.hasNext()) {
                    reputDelayMessageCallback(it.next());
                }
            }
        });
    }

    private void doReputAsync(DelayMessageInner msg) {
        reputExpeiredMessageService.submit(new Runnable() {
            @Override
            public void run() {
                reputDelayMessageCallback(msg);
            }
        });
    }

    private void reputDelayMessageCallback(DelayMessageInner msg) {
        reputExpiredMessageCallback.callback(msg);
    }

    public void shutdown() {
        // TODO shutdown
        loadMessageManager.shutdown();
    }

}
