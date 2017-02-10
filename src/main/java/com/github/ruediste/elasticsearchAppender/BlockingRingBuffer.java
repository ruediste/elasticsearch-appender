package com.github.ruediste.elasticsearchAppender;

import java.time.Duration;
import java.time.Instant;

public class BlockingRingBuffer {

    public Object[] elements = null;

    private int capacity = 0;
    private int writePos = 0;
    private int available = 0;

    public BlockingRingBuffer(int capacity) {
        this.capacity = capacity;
        this.elements = new Object[capacity];
    }

    public synchronized boolean put(Object element) {
        if (available < capacity) {
            if (writePos >= capacity) {
                writePos = 0;
            }
            if (available == 0)
                notifyAll();
            elements[writePos] = element;
            writePos++;
            available++;
            return true;
        } else {
            // discard
            return false;
        }
    }

    public synchronized Object take() {
        while (available == 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (available == this.capacity) {
            // notify all waiting threads, since the queue was full before
            notifyAll();
        }

        int nextSlot = writePos - available;
        if (nextSlot < 0) {
            nextSlot += capacity;
        }
        Object nextObj = elements[nextSlot];
        available--;
        return nextObj;

    }

    /**
     * 
     * @param maxCount
     * @param maxWait
     *            duration to wait maximally for at least one element to become
     *            available, null for infinite wait, {@link Duration#ZERO} for
     *            no waiting
     * @return
     */
    public synchronized Object[] drain(int maxCount, Duration maxWait) {

        waitUntilElementsAvailable(maxWait);
        if (available == 0)
            return new Object[] {};

        if (available == this.capacity) {
            // notify all waiting threads, since the queue was full before
            notifyAll();
        }

        int firstSlot = writePos - available;
        if (firstSlot < 0)
            firstSlot += capacity;

        int resultLength = Math.min(available, maxCount);
        Object[] result = new Object[resultLength];

        if (firstSlot + resultLength < capacity) {
            // all elements are in one chunk
            System.arraycopy(elements, firstSlot, result, 0, resultLength);
        } else {
            // there is a wrap around

            // copy the elements up to the end of elements
            int firstChunkLength = capacity - firstSlot;
            System.arraycopy(elements, firstSlot, result, 0, firstChunkLength);

            // copy the remaining elements from the start of elements
            System.arraycopy(elements, 0, result, firstChunkLength, resultLength - firstChunkLength);
        }
        available -= resultLength;
        return result;

    }

    private void waitUntilElementsAvailable(Duration maxWait) {
        Duration actualWait;
        if (maxWait == null) {
            // wait forever
            actualWait = Duration.ZERO;
        } else if (maxWait.isZero()) {
            // no waiting
            return;
        } else {
            actualWait = maxWait;
        }

        if (available == 0) {
            Instant now = Instant.now();
            Instant end = now.plus(actualWait);
            while (true) {
                try {
                    wait(Duration.between(now, end).toMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (available > 0)
                    break;
                now = Instant.now();
                if (now.isAfter(end))
                    break;
            }
        }
    }

    public void reset() {
        this.writePos = 0;
        this.available = 0;
    }

    public int capacity() {
        return this.capacity;
    }

    public int available() {
        return this.available;
    }

    public int remainingCapacity() {
        return this.capacity - this.available;
    }

}
