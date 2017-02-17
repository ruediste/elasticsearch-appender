package com.github.ruediste.elasticsearchAppender.log4j;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;

import com.github.ruediste.elasticsearchAppender.EsAppenderHelper;
import com.github.ruediste.elasticsearchAppender.EsAppenderHelperProps;
import com.github.ruediste.elasticsearchAppender.EsIndexer;
import com.github.ruediste.elasticsearchAppender.EsIndexerLogger;
import com.github.ruediste.elasticsearchAppender.EsIndexerProps;
import com.github.ruediste.elasticsearchAppender.EsLogRecord;

@Plugin(name = "EsAppender", category = "Core", elementType = "appender", printObject = true)
public final class EsAppenderLog4j2 extends AbstractAppender {

    protected EsAppenderLog4j2(String name, Filter filter, Layout<? extends Serializable> layout,
            boolean ignoreExceptions) {
        super(name, filter, layout, ignoreExceptions);
    }

    private EsIndexer indexer = new EsIndexer();
    private EsAppenderHelper helper = new EsAppenderHelper();
    private String esType = "log";

    @PluginBuilderFactory
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements org.apache.logging.log4j.core.util.Builder<EsAppenderLog4j2>, EsIndexerProps,
            EsAppenderHelperProps {

        EsIndexer defIndexer = new EsIndexer();
        EsAppenderHelper defHelper = new EsAppenderHelper();

        @Override
        public EsAppenderLog4j2 build() {
            EsAppenderLog4j2 appender = new EsAppenderLog4j2(name, filter, layout, ignoreExceptions);
            appender.esType = esType;
            appender.helper.maxStringLength = maxStringLength;
            appender.helper.indexPattern = indexPattern;
            appender.helper.tags = tags;
            appender.helper.labels = labels;
            appender.indexer.threadName = threadName;
            appender.indexer.capacity = capacity;
            appender.indexer.maxBulkDocumentCount = maxBulkDocumentCount;
            appender.indexer.maxBulkMemorySize = maxBulkMemorySize;
            appender.indexer.stopTimeout = Duration.parse(stopTimeout);
            appender.indexer.failurePause = Duration.parse(failurePause);
            appender.indexer.esUrl = esUrl;
            appender.indexer.performJMXRegistration = performJMXRegistration;
            appender.indexer.mBeanName = mBeanName;
            appender.indexer.slidingWindowSlotSize = slidingWindowSlotSize;
            appender.indexer.slidingWindowSlotCount = slidingWindowSlotCount;

            return appender;
        }

        @PluginBuilderAttribute
        boolean ignoreExceptions;

        public void setIgnoreExceptions(boolean ignoreExceptions) {
            this.ignoreExceptions = ignoreExceptions;
        }

        @PluginBuilderAttribute
        @Required(message = "No name provided for ListAppender")
        private String name;

        @PluginElement("Layout")
        private Layout<? extends Serializable> layout;

        @PluginElement("Filter")
        private Filter filter;

        @Override
        public void setName(String name) {
            this.name = name;
        }

        public Builder setLayout(final Layout<? extends Serializable> layout) {
            this.layout = layout;
            return this;
        }

        public Builder setFilter(final Filter filter) {
            this.filter = filter;
            return this;
        }

        @PluginBuilderAttribute
        String esType = "log";

        public void setEsType(String esType) {
            this.esType = esType;
        }

        @PluginBuilderAttribute
        int maxStringLength = defHelper.maxStringLength;

        @Override
        public void setMaxStringLength(String maxStringLength) {
            this.maxStringLength = Integer.valueOf(maxStringLength);
        }

        @PluginBuilderAttribute
        String indexPattern = defHelper.indexPattern;

        @Override
        public void setIndexPattern(String indexPattern) {
            this.indexPattern = indexPattern;
        }

        @PluginBuilderAttribute
        String tags = defHelper.tags;

        @Override
        public void setTags(String tags) {
            this.tags = tags;
        }

        @PluginBuilderAttribute
        String labels = defHelper.labels;

        @Override
        public void setLabels(String labels) {
            this.labels = labels;
        }

        @PluginBuilderAttribute
        String threadName = defIndexer.threadName;

        @Override
        public void setThreadName(String threadName) {
            this.threadName = threadName;
        }

        @PluginBuilderAttribute
        int capacity = defIndexer.capacity;

        @Override
        public void setCapacity(String capacity) {
            this.capacity = EsIndexer.parseMemorySizeValue(capacity);
        }

        @PluginBuilderAttribute
        int maxBulkDocumentCount = defIndexer.maxBulkDocumentCount;

        @Override
        public void setMaxBulkDocumentCount(String maxBulkDocumentCount) {
            this.maxBulkDocumentCount = Integer.valueOf(maxBulkDocumentCount);
        }

        @PluginBuilderAttribute
        int maxBulkMemorySize = defIndexer.maxBulkMemorySize;

        @Override
        public void setMaxBulkMemorySize(String maxBulkMemorySize) {
            this.maxBulkMemorySize = EsIndexer.parseMemorySizeValue(maxBulkMemorySize);
        }

        @PluginBuilderAttribute
        String stopTimeout = defIndexer.stopTimeout.toString();

        @Override
        public void setStopTimeout(String stopTimeout) {
            this.stopTimeout = stopTimeout;
        }

        @PluginBuilderAttribute
        String failurePause = defIndexer.failurePause.toString();

        @Override
        public void setFailurePause(String failurePause) {
            this.failurePause = failurePause;
        }

        @PluginBuilderAttribute
        String esUrl = defIndexer.esUrl;

        @Override
        public void setEsUrl(String esUrl) {
            this.esUrl = esUrl;
        }

        @PluginBuilderAttribute
        boolean performJMXRegistration = defIndexer.performJMXRegistration;

        @Override
        public void setPerformJMXRegistration(String performJMXRegistration) {
            this.performJMXRegistration = Boolean.valueOf(performJMXRegistration);
        }

        @PluginBuilderAttribute
        String mBeanName = defIndexer.mBeanName;

        @Override
        public void setmBeanName(String mBeanName) {
            this.mBeanName = mBeanName;
        }

        @PluginBuilderAttribute
        long slidingWindowSlotSize = defIndexer.slidingWindowSlotSize;

        @Override
        public void setSlidingWindowSlotSize(String size) {
            this.slidingWindowSlotSize = Long.valueOf(size);
        }

        @PluginBuilderAttribute
        int slidingWindowSlotCount = defIndexer.slidingWindowSlotCount;

        @Override
        public void setSlidingWindownSlotCount(String count) {
            this.slidingWindowSlotCount = Integer.valueOf(count);
        }

        @Override
        public EsAppenderHelper getHelper() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EsIndexer getIndexer() {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public void append(LogEvent event) {
        EsLogRecord record = new EsLogRecord();
        helper.prepareLogRecord(record, event.getTimeMillis());
        record.thread = event.getThreadName();
        record.logger = event.getLoggerName();
        record.message = helper.truncate(event.getMessage().getFormattedMessage());
        record.level = event.getLevel().toString();

        Throwable t = event.getThrown();
        if (t != null) {
            record.exceptionClass = t.getClass().getName();
            record.exceptionMessage = helper.truncate(t.getMessage());
            record.stackTrace = helper.getStackTrace(t);
        }
        {
            Map<String, String> contextMap = event.getContextMap();
            if (!contextMap.isEmpty()) {
                record.mdc = new HashMap<>();
                for (Entry<?, ?> entry : contextMap.entrySet()) {
                    record.mdc.put(helper.truncate(String.valueOf(entry.getKey())).replace('.', '_'),
                            helper.truncate(String.valueOf(entry.getValue())));
                }
            }
        }
        indexer.queue(helper.getIndex(record.time), esType, record);
    }

    @Override
    public void start() {
        helper.start();
        indexer.name = getName();
        Logger log = LogManager.getLogger(EsIndexer.class);
        indexer.logger = new EsIndexerLogger() {

            @Override
            public void warn(String message) {
                log.warn(message);
            }

            @Override
            public void info(String message) {
                log.info(message);
            }

            @Override
            public void error(String message) {
                log.error(message);
            }
        };
        indexer.start();
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        indexer.stop();
        helper.stop();
    }

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
    }

}