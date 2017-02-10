package com.github.ruediste.elasticsearchAppender;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BlockingRingBuffer {

    public byte[] buffer = null;

    private int capacity = 0;
    private int writePos = 0;
    private int available = 0;

    public BlockingRingBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = new byte[capacity];
    }

    /**
     * Put an element into the buffer.
     * 
     * @param elementParts
     *            parts of the element. The individual arrays will be
     *            concatenated
     * @return true if the element has been added, false if there was no space
     *         remaining
     */
    public boolean put(byte[]... elementParts) {
        int elementLengthSum = 0;
        for (byte[] element : elementParts) {
            elementLengthSum += element.length;
        }
        synchronized (this) {
            int lengthByteCount = 1;
            if (available + elementLengthSum + lengthByteCount <= capacity) {
                // TODO: proper handling of length
                appendBytes(ByteBuffer.allocate(lengthByteCount).put((byte) elementLengthSum).array());

                for (byte[] element : elementParts) {
                    appendBytes(element);
                }

                return true;
            } else {
                // discard
                return false;
            }
        }
    }

    private void appendBytes(byte[] element) {
        if (writePos >= capacity) {
            writePos = 0;
        }
        if (writePos + element.length > capacity) {
            // need to split
            int firstPart = capacity - writePos;
            int secondPart = element.length - firstPart;
            System.arraycopy(element, 0, buffer, writePos, firstPart);
            System.arraycopy(element, firstPart, buffer, 0, secondPart);
            writePos += secondPart;
        } else {
            System.arraycopy(element, 0, buffer, writePos, element.length);
            writePos += element.length;
        }
        if (available == 0)
            notifyAll();
        available += element.length;
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
    public synchronized List<byte[]> drain(int maxCount, Duration maxWait) {

        waitUntilElementsAvailable(maxWait);
        if (available == 0)
            return Collections.emptyList();

        if (available == this.capacity) {
            // notify all waiting threads, since the queue was full before
            notifyAll();
        }

        List<byte[]> result = new ArrayList<>();

        int firstSlot = writePos - available;
        if (firstSlot < 0)
            firstSlot += capacity;

        do {
            // TODO: proper handling of length
            int elementLength = buffer[firstSlot];
            available--;

            firstSlot++;
            if (firstSlot >= capacity)
                firstSlot -= capacity;

            // read element
            byte[] element = new byte[elementLength];

            if (firstSlot + elementLength <= capacity) {
                // all elements are in one chunk
                System.arraycopy(buffer, firstSlot, element, 0, elementLength);
                firstSlot += elementLength;
            } else {
                // there is a wrap around

                // copy the elements up to the end of elements
                int firstChunkLength = capacity - firstSlot;
                int secondChunkLength = elementLength - firstChunkLength;

                System.arraycopy(buffer, firstSlot, element, 0, firstChunkLength);

                // copy the remaining elements from the start of elements
                System.arraycopy(buffer, 0, element, firstChunkLength, secondChunkLength);
                firstSlot = secondChunkLength;
            }
            if (firstSlot >= capacity)
                firstSlot -= capacity;
            result.add(element);
            available -= elementLength;
        } while (available > 0 && result.size() < maxCount);

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
