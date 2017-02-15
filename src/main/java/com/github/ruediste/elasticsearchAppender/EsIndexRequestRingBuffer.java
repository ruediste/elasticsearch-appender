package com.github.ruediste.elasticsearchAppender;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Ring buffer designed to store serialized log events
 */
public class EsIndexRequestRingBuffer {

    private byte[] buffer = null;

    private final int capacity;
    private int writePos = 0;
    private int available = 0;
    private int availableElements = 0;

    public EsIndexRequestRingBuffer(int capacity) {
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
        byte[] lengthBB = intToBytes(elementLengthSum);
        synchronized (this) {
            if (available + elementLengthSum + lengthBB.length <= capacity) {
                appendBytes(lengthBB);

                for (byte[] element : elementParts) {
                    appendBytes(element);
                }

                availableElements++;
                return true;
            } else {
                // discard
                return false;
            }
        }
    }

    private void appendBytes(byte[] element) {
        if (writePos + element.length > capacity) {
            // need to split
            int firstPart = capacity - writePos;
            int secondPart = element.length - firstPart;
            System.arraycopy(element, 0, buffer, writePos, firstPart);
            System.arraycopy(element, firstPart, buffer, 0, secondPart);
            writePos = secondPart;
        } else {
            System.arraycopy(element, 0, buffer, writePos, element.length);
            writePos += element.length;
        }
        if (writePos >= capacity)
            writePos -= capacity;
        if (available == 0)
            notifyAll();
        available += element.length;
    }

    /**
     * 
     * @param maxCount
     * @param maxSize
     *            maximum number of bytes to drain from the buffer (sum of
     *            drained elements). Ignored for the first element retrieved,
     *            such that single long elements cannot block the whole buffer.
     *            -1 for no limit
     * @param maxWait
     *            duration to wait maximally for at least one element to become
     *            available, null for infinite wait, {@link Duration#ZERO} for
     *            no waiting
     * @return
     */
    public synchronized List<byte[]> drain(int maxCount, int maxSize, Duration maxWait) {

        waitUntilElementsAvailable(maxWait);
        if (available == 0)
            return Collections.emptyList();

        if (available == this.capacity) {
            // notify all waiting threads, since the queue was full before
            notifyAll();
        }

        List<byte[]> result = new ArrayList<>();

        int elementLengthSum = 0;

        do {

            int elementLength;
            {
                int oldAvailable = available;
                elementLength = readInt();
                elementLengthSum += elementLength;

                // don't break when retrieving first element
                if (maxSize >= 0 && !result.isEmpty() && elementLengthSum > maxSize) {
                    available = oldAvailable;
                    break;
                }
            }

            int firstSlot = writePos - available;
            if (firstSlot < 0)
                firstSlot += capacity;

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
            result.add(element);
            available -= elementLength;
            availableElements--;
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
                    long millis = Duration.between(now, end).toMillis();
                    if (millis > 0 || maxWait == null)
                        wait(millis);
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

    public synchronized void reset() {
        this.writePos = 0;
        this.available = 0;
        availableElements = 0;
    }

    public synchronized double usedCapacityFraction() {
        return ((double) available) / capacity;
    }

    public synchronized int capacity() {
        return this.capacity;
    }

    public synchronized int available() {
        return this.available;
    }

    public synchronized int availableElements() {
        return this.availableElements;
    }

    public int remainingCapacity() {
        return this.capacity - this.available;
    }

    void setBufferContents(byte[] bb) {
        System.arraycopy(bb, 0, buffer, 0, bb.length);
        writePos = bb.length;
        available = bb.length;
    }

    int readInt() {
        int firstSlot = writePos - available;
        if (firstSlot < 0)
            firstSlot += capacity;

        // first byte
        byte b = buffer[firstSlot++];
        available--;
        if ((b & 0x80) == 0) {
            return b;
        }
        int result = b & 0x7f;

        // second byte
        if (firstSlot >= capacity)
            firstSlot -= capacity;
        b = buffer[firstSlot++];
        available--;
        result |= (b & 0x7f) << 7;
        if ((b & 0x80) == 0) {
            return result;
        }

        // remaining bytes
        if (firstSlot >= capacity)
            firstSlot -= capacity;
        b = buffer[firstSlot++];
        available--;
        result |= (b << 14) & (0xff << 14);

        if (firstSlot >= capacity)
            firstSlot -= capacity;
        b = buffer[firstSlot++];
        available--;
        result |= (b << 22) & (0xff << 22);

        return result;
    }

    byte[] intToBytes(int i) {
        int tmp = i;
        int first = tmp & 0x7F;
        tmp &= ~0x7F;
        if (tmp == 0) {
            byte[] result = new byte[1];
            result[0] = (byte) first;
            return result;
        }
        tmp >>>= 7;
        first |= 0x80;

        int second = tmp & 0x7F;
        tmp &= ~0x7F;
        if (tmp == 0) {
            byte[] result = new byte[2];
            result[0] = (byte) first;
            result[1] = (byte) second;
            return result;
        }
        tmp >>>= 7;
        second |= 0x80;

        int third = tmp & 0xFFFF;
        tmp &= ~0xFFFF;
        if (tmp == 0) {
            byte[] result = new byte[4];
            result[0] = (byte) first;
            result[1] = (byte) second;
            result[2] = (byte) third;
            result[3] = (byte) (third >>> 8);
            return result;
        }
        throw new ArithmeticException("Length is more than 2**29");
    }

}
