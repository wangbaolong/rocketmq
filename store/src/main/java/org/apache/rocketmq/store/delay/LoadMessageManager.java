package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class LoadMessageManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    private DelayMessageStore delayMessageStore;
    private TimingWheelBucket[] currentBuckets;
    private TimingWheelBucket[] preLoadBuckets;
    private ExecutorService loadMessageService;
    private AtomicBoolean preloadStarted = new AtomicBoolean(false);
    private Future<TimingWheelBucket[]> preloadFuture;
    private Future<TimingWheelBucket[]> currentFuture;

    public LoadMessageManager(DelayMessageStore delayMessageStore) {
        this.delayMessageStore = delayMessageStore;
        initLoadMessageServicer();
    }

    private void initLoadMessageServicer() {
        loadMessageService = new ThreadPoolExecutor(1,
                1,
                60 , TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new DelayThreadFactory("loadMessageService"),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        log.error("OverflowTimingWheel Thread pool rejected");
                    }
                });
    }

    public void initBuckets(int wheelSize, long currentTime) {
        for (int i = 0; i < wheelSize; i++) {
            this.currentBuckets[i] = new TimingWheelBucket();
            this.preLoadBuckets[i] = new TimingWheelBucket();
        }
        currentFuture = loadMessageService.submit(new Callable<TimingWheelBucket[]>() {
            @Override
            public TimingWheelBucket[] call() throws Exception {

                return new TimingWheelBucket[0];
            }
        });
    }

    public TimingWheelBucket[] getCurrentBuckets() {
        try {
            if (currentFuture == null) {
                throw new NullPointerException("Uninitialized Buckets");
            }
            return currentFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public TimingWheelBucket[] getPreloadBuckets() {
        return null;
    }

    public void startPreloadBuckets(long startMs, long tickMs, int wheelSize) {
        if (preloadStarted.compareAndSet(false, true)) {
            preloadFuture = preloadFuture = loadMessageService.submit(new Callable<TimingWheelBucket[]>() {
                @Override
                public TimingWheelBucket[] call() {
                    //startMs
                    try {
                        delayMessageStore.loadDelayMessageFromStoreToTimingWheel(startMs,
                                new MyLoadDelayMessageCallback(preLoadBuckets, startMs, tickMs, wheelSize));
                        return preLoadBuckets;
                    } catch (Exception e){
                        log.info("Start preload buckets {}", e.getMessage());
                        return null;
                    } finally {
                        preloadStarted.set(false);
                    }
                }
            });
        }
    }

    public void handleExpiredMessage(long currentTime, ReputExpiredMessageCallback callback) {

    }

    private class MyLoadDelayMessageCallback implements DelayMessageQueue.LoadDelayMessageCallback {

        TimingWheelBucket[] buckets;
        long startMs;
        long tickMs;
        int wheelSize;


        public MyLoadDelayMessageCallback(TimingWheelBucket[] buckets, long startMs, long ticks, int wheelSize) {
            this.buckets = buckets;
            this.startMs = startMs;
            this.tickMs = ticks;
            this.wheelSize = wheelSize;
        }

        @Override
        public void callback(DelayMessageInner msgInner) {
            long virtualId = (msgInner.getExpirationMs() - startMs) / tickMs;
            int index = (int) virtualId % wheelSize - 1;
            TimingWheelBucket bucket = buckets[index];
            bucket.addDelayMessage(msgInner);
        }
    }

}
