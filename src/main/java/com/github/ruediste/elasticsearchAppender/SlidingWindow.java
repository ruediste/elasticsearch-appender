package com.github.ruediste.elasticsearchAppender;

import java.util.Arrays;

/**
 * Implements a sliding window for counting events
 */
public class SlidingWindow {

    private long slotSize;
    private int slotCount;

    private int slotHead = 0;
    private long[] slots;
    private long slotSum;

    private long currentCount;
    private long currentSlotNr;

    public SlidingWindow(long slotSize, int slotCount) {
        this.slotSize = slotSize;
        this.slotCount = slotCount;
        slots = new long[slotCount];
    }

    synchronized public long getEventCount(long timeStamp) {
        updateTime(timeStamp);
        return slotSum;
    }

    synchronized public void addEvent(long timeStamp) {
        updateTime(timeStamp);
        currentCount++;
    }

    synchronized public void addEvents(long timeStamp, long count) {
        updateTime(timeStamp);
        currentCount += count;
    }

    private void updateTime(long timeStamp) {
        long slotNr = timeStamp / slotSize;
        if (slotNr > currentSlotNr) {
            // we need to shift
            if (slotNr - currentSlotNr > slotCount) {
                Arrays.fill(slots, 0);
                slotHead = 0;
                currentSlotNr = slotNr;
                currentCount = 0;
                slotSum = 0;
            } else {
                slotSum += currentCount - slots[slotHead];
                slots[slotHead] = currentCount;
                slotHead = (slotHead + 1) % slotCount;

                for (int i = 1; i < slotNr - currentSlotNr; i++) {
                    slotSum -= slots[slotHead];
                    slots[slotHead] = 0;
                    slotHead = (slotHead + 1) % slotCount;
                }
                currentSlotNr = slotNr;
                currentCount = 0;
            }
        }
    }
}