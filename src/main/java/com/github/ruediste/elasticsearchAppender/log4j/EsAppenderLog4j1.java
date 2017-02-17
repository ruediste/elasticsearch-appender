package com.github.ruediste.elasticsearchAppender.log4j;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map.Entry;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import com.github.ruediste.elasticsearchAppender.EsAppenderHelper;
import com.github.ruediste.elasticsearchAppender.EsAppenderHelperProps;
import com.github.ruediste.elasticsearchAppender.EsIndexer;
import com.github.ruediste.elasticsearchAppender.EsIndexerLogger;
import com.github.ruediste.elasticsearchAppender.EsIndexerProps;
import com.github.ruediste.elasticsearchAppender.EsLogRecord;

import ch.qos.logback.classic.pattern.ThrowableProxyConverter;

public class EsAppenderLog4j1 extends AppenderSkeleton implements EsIndexerProps, EsAppenderHelperProps {

    private EsIndexer indexer = new EsIndexer();
    private EsAppenderHelper helper = new EsAppenderHelper();
    private ThrowableProxyConverter throwableProxyConverter = new ThrowableProxyConverter();
    private String esType = "log";

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    protected void append(LoggingEvent event) {
        if (!isAsSevereAsThreshold(event.getLevel()))
            return;

        EsLogRecord record = new EsLogRecord();
        helper.prepareLogRecord(record, event.getTimeStamp());
        record.thread = event.getThreadName();
        record.logger = event.getLoggerName();
        record.message = helper.truncate(event.getRenderedMessage());
        record.level = event.getLevel().toString();

        ThrowableInformation ti = event.getThrowableInformation();
        if (ti != null) {
            Throwable t = ti.getThrowable();
            if (t != null) {
                record.exceptionClass = t.getClass().getName();
                record.exceptionMessage = helper.truncate(t.getMessage());
                record.stackTrace = helper.getStackTrace(t);
            }
        }
        Hashtable<?, ?> context = org.apache.log4j.MDC.getContext();
        if (context != null) {
            record.mdc = new HashMap<>();
            for (Entry<?, ?> entry : context.entrySet()) {
                record.mdc.put(helper.truncate(String.valueOf(entry.getKey())).replace('.', '_'),
                        helper.truncate(String.valueOf(entry.getValue())));
            }
        }
        indexer.queue(helper.getIndex(record.time), esType, record);
    }

    @Override
    public void activateOptions() {
        throwableProxyConverter.start();
        helper.start();
        indexer.name = getName();
        Logger log = Logger.getLogger(EsIndexer.class);
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
    }

    @Override
    public void close() {
        indexer.stop();
        throwableProxyConverter.stop();
        helper.stop();
    }

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
    }

    @Override
    public EsAppenderHelper getHelper() {
        return helper;
    }

    @Override
    public EsIndexer getIndexer() {
        return indexer;
    }

    @Override
    public void setMaxStringLength(String maxStringLength) {
        EsAppenderHelperProps.super.setMaxStringLength(maxStringLength);
    }

    @Override
    public void setIndexPattern(String indexPattern) {
        EsAppenderHelperProps.super.setIndexPattern(indexPattern);
    }

    @Override
    public void setTags(String tags) {
        EsAppenderHelperProps.super.setTags(tags);
    }

    @Override
    public void setLabels(String labels) {
        EsAppenderHelperProps.super.setLabels(labels);
    }

    @Override
    public void setThreadName(String threadName) {
        EsIndexerProps.super.setThreadName(threadName);
    }

    @Override
    public void setCapacity(String capacity) {
        EsIndexerProps.super.setCapacity(capacity);
    }

    @Override
    public void setMaxBulkDocumentCount(String maxBulkDocumentCount) {
        EsIndexerProps.super.setMaxBulkDocumentCount(maxBulkDocumentCount);
    }

    @Override
    public void setMaxBulkMemorySize(String maxBulkMemorySize) {
        EsIndexerProps.super.setMaxBulkMemorySize(maxBulkMemorySize);
    }

    @Override
    public void setStopTimeout(String stopTimeout) {
        EsIndexerProps.super.setStopTimeout(stopTimeout);
    }

    @Override
    public void setEsUrl(String esUrl) {
        EsIndexerProps.super.setEsUrl(esUrl);
    }

    @Override
    public void setPerformJMXRegistration(String performJMXRegistration) {
        EsIndexerProps.super.setPerformJMXRegistration(performJMXRegistration);
    }

    @Override
    public void setmBeanName(String mBeanName) {
        EsIndexerProps.super.setmBeanName(mBeanName);
    }

    @Override
    public void setSlidingWindowSlotSize(String size) {
        EsIndexerProps.super.setSlidingWindowSlotSize(size);
    }

    @Override
    public void setSlidingWindownSlotCount(String count) {
        EsIndexerProps.super.setSlidingWindownSlotCount(count);
    }

}