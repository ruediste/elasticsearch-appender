package com.github.ruediste.elasticsearchAppender;

public interface EsIndexerMBean {
    public long getTotalEventIndexedCount();

    public long getTotalEventDiscardedCount();

    public long getTotalEventIndexingFailedCount();

    public long getQueueLength();

    public double getQueueFillFraction();

    public void resetStatistics();
}
