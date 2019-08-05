package org.apache.rocketmq.store.delay;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

public class TimingWheelBucket {

    private DelayMessageInner head;
    private DelayMessageInner tail;
    private ReentrantLock lock = new ReentrantLock();

    public TimingWheelBucket() {
    }

    public void addDelayMessage(DelayMessageInner msg) {
        try {
            lock.lock();
            if (head == null) {
                head = msg;
                tail = msg;
            } else {
                tail.next = msg;
                msg.prev = tail;
                tail = msg;
            }
        } finally {
            lock.unlock();
        }
    }

    public void reset() {
        try {
            lock.lock();
            head = null;
            tail = null;
        } finally {
            lock.unlock();
        }
    }

    public Iterator<DelayMessageInner> getDelayMessageListAndResetBucket() {
       try {
            lock.lock();
            DelayMessageInner tempHead = head;
            reset();
            return new BucketIterator(tempHead);
        } finally {
            lock.unlock();
        }
    }

    private class BucketIterator implements Iterator<DelayMessageInner> {

        DelayMessageInner head;

        BucketIterator(DelayMessageInner head) {
            this.head = head;
        }


        @Override
        public boolean hasNext() {
            return head != null;
        }

        @Override
        public DelayMessageInner next() {
            DelayMessageInner tempHead = head;
            head = head.next;
            return tempHead;
        }
    }

}
