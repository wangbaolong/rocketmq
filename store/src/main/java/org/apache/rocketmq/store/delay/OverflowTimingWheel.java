package org.apache.rocketmq.store.delay;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OverflowTimingWheel {

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
    private TimingWheel.ReputExpiredMessageCallback reputExpiredMessageCallback;

    public OverflowTimingWheel(long tickMs, int wheelSize, long startMs) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.startMs = startMs;
        this.currentTime = startMs - (startMs % tickMs);
    }

    public
}
