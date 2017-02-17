package com.github.ruediste.elasticsearchAppender;

public interface EsIndexerMBean {
    public long getTotalEventIndexedCount();

    public long getEventIndexedCount();

    public long getTotalEventDiscardedCount();

    public long getEventDiscardedCount();

    public long getTotalEventIndexingFailedCount();

    public long getEventIndexingFailedCount();

    public long getTotalEventLostCount();

    public long getEventLostCount();

    public long getQueueLength();

    public double getQueueFillFraction();

    public void resetStatistics();
}
