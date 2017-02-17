package com.github.ruediste.elasticsearchAppender;

import java.time.Duration;

public interface EsIndexerProps {
    EsIndexer getIndexer();

    default void setThreadName(String threadName) {
        getIndexer().threadName = threadName;
    }

    default void setCapacity(String capacity) {
        getIndexer().capacity = Integer.valueOf(capacity);
    }

    default void setMaxBulkDocumentCount(String maxBulkDocumentCount) {
        getIndexer().maxBulkDocumentCount = Integer.valueOf(maxBulkDocumentCount);
    }

    default void setMaxBulkMemorySize(String maxBulkMemorySize) {
        getIndexer().maxBulkMemorySize = Integer.valueOf(maxBulkMemorySize);
    }

    default void setStopTimeout(String stopTimeout) {
        getIndexer().stopTimeout = Duration.parse(stopTimeout);
    }

    default void setEsUrl(String esUrl) {
        getIndexer().esUrl = esUrl;
    }

    default void setPerformJMXRegistration(String performJMXRegistration) {
        getIndexer().performJMXRegistration = Boolean.valueOf(performJMXRegistration);
    }

    default void setmBeanName(String mBeanName) {
        getIndexer().mBeanName = mBeanName;
    }

    default void setName(String name) {
        getIndexer().name = name;
    }

    default void setSlidingWindowSlotSize(String size) {
        getIndexer().slidingWindowSlotSize = Long.valueOf(size);
    }

    default void setSlidingWindownSlotCount(String count) {
        getIndexer().slidingWindowSlotCount = Integer.valueOf(count);
    }

}
