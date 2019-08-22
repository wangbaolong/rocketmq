package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadMessageManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    private final int PRELOAD_STATUS_IDEA = 0;
    private final int PRELOAD_STATUS_LOADING = 1;
    private final int PRELOAD_STATUS_LOAD_END = 2;
    private final int PRELOAD_STATUS_GETTING_BUCKETS = 3;

    private DelayMessageStore delayMessageStore;
    private TimingWheelBucket[] currentBuckets;
    private TimingWheelBucket[] preLoadBuckets;
    private ExecutorService loadMessageService;
    private Future<TimingWheelBucket[]> preloadFuture;
    private AtomicInteger preloadStatus = new AtomicInteger(0);

    private long startMs;
    private long tickMs;
    private int wheelSize;
    private long interval;

    public LoadMessageManager(DelayMessageStore delayMessageStore,
                              long startMs,
                              long tickMs,
                              int wheelSize,
                              long interval) {
        this.delayMessageStore = delayMessageStore;
        this.startMs = startMs;
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.interval = interval;

        initLoadMessageService();

        for (int i = 0; i < wheelSize; i++) {
            this.currentBuckets[i] = new TimingWheelBucket();
            this.preLoadBuckets[i] = new TimingWheelBucket();
        }
    }

    private void initLoadMessageService() {
        loadMessageService = new ThreadPoolExecutor(3,
                3,
                60 , TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new DelayThreadFactory("loadMessageService"),
                    new RejectedExecutionHandler() {
                        @Override
                        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                            log.error("OverflowTimingWheel Thread pool rejected");
                        }
                    }
                );
    }

    public void resetstartMs(long startMs) {
        this.startMs = startMs;
    }

    public TimingWheelBucket[] getCurrentBuckets(long startMs, long tickMs, int wheelSize, long currentTime) {
        try {
            Future<TimingWheelBucket[]> currentFuture = loadMessageService.submit(new Callable<TimingWheelBucket[]>() {
                @Override
                public TimingWheelBucket[] call() throws Exception {
                    delayMessageStore.loadDelayMessageFromStoreToTimingWheel(startMs,
                            new DelayMessageQueue.LoadDelayMessageCallback() {
                                @Override
                                public void callback(DelayMessageInner msgInner) {
                                    if (msgInner.getExpirationMs() > currentTime) {
                                        long virtualId = (msgInner.getExpirationMs() - startMs) / tickMs;
                                        int index = (int) virtualId % wheelSize - 1;
                                        TimingWheelBucket bucket = preLoadBuckets[index];
                                        bucket.addDelayMessage(msgInner);
                                    }
                                }
                            });
                    return currentBuckets;
                }
            });
            return currentFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public TimingWheelBucket[] getPreloadBuckets() {
        log.info("LoadMessageManager getPreloadBuckets current status:{}", preloadStatus.get());
        try {
            if (preloadFuture != null) {
                TimingWheelBucket[] buckets = preloadFuture.get();
                if (preloadStatus.compareAndSet(PRELOAD_STATUS_LOAD_END, PRELOAD_STATUS_GETTING_BUCKETS)) {
                    preLoadBuckets = currentBuckets;
                    currentBuckets = buckets;
                    preloadFuture = null;
                    preloadStatus.set(PRELOAD_STATUS_IDEA);
                    return buckets;
                }
            } else {
                log.info("[BUG] LoadMessageManager getPreloadBuckets preloadFuture is null");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void putMessage(DelayMessageInner msgInner) {
        log.info("LoadMessageManager putMessage current status:{}", preloadStatus.get());
        if (preloadStatus.get() == PRELOAD_STATUS_LOAD_END
                && msgInner.getExpirationMs() < startMs + interval) {
            putMessage0(msgInner);
        }
    }

    private void putMessage0(DelayMessageInner msgInner) {
        long virtualId = (msgInner.getExpirationMs() - startMs) / tickMs;
        int index = (int) virtualId % wheelSize - 1;
        TimingWheelBucket bucket = preLoadBuckets[index];
        bucket.addDelayMessage(msgInner);
    }

    public void startPreloadBuckets() {
        log.info("LoadMessageManager startPreloadBuckets current status:{}", preloadStatus.get());
        if (preloadStatus.compareAndSet(PRELOAD_STATUS_IDEA, PRELOAD_STATUS_LOADING)) {
            preloadFuture = loadMessageService.submit(new Callable<TimingWheelBucket[]>() {
                @Override
                public TimingWheelBucket[] call() {
                    //startMs
                    try {
                        delayMessageStore.loadDelayMessageFromStoreToTimingWheel(startMs,
                                new DelayMessageQueue.LoadDelayMessageCallback() {
                                    @Override
                                    public void callback(DelayMessageInner msgInner) {
                                        putMessage0(msgInner);
                                    }
                                });
                        return preLoadBuckets;
                    } catch (Exception e){
                        log.info("Start preload error {}", e.getMessage());
                        return null;
                    } finally {
                        preloadStatus.set(PRELOAD_STATUS_LOAD_END);
                    }
                }
            });
        }
    }

    public void startHandleExpiredMessage(long currentTime, ReputExpiredMessageCallback callback) {
        loadMessageService.submit(new Runnable() {
            @Override
            public void run() {

            }
        });
    }

}
