package com.github.ruediste.elasticsearchAppender.logback;

import java.util.HashMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ruediste.elasticsearchAppender.EsAppenderHelper;
import com.github.ruediste.elasticsearchAppender.EsAppenderHelperProps;
import com.github.ruediste.elasticsearchAppender.EsIndexer;
import com.github.ruediste.elasticsearchAppender.EsIndexerLogger;
import com.github.ruediste.elasticsearchAppender.EsIndexerProps;
import com.github.ruediste.elasticsearchAppender.EsLogRecord;

import ch.qos.logback.classic.pattern.ThrowableProxyConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.UnsynchronizedAppenderBase;

public class EsAppenderLogback extends UnsynchronizedAppenderBase<ILoggingEvent>
        implements EsIndexerProps, EsAppenderHelperProps {

    EsIndexer indexer = new EsIndexer();
    private EsAppenderHelper helper = new EsAppenderHelper();
    private ThrowableProxyConverter throwableProxyConverter = new ThrowableProxyConverter();
    private String esType = "log";

    @Override
    public void start() {
        super.start();
        helper.start();
        throwableProxyConverter.start();
        indexer.name = getName();
        Logger log = LoggerFactory.getLogger(EsIndexer.class);
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
    public void stop() {
        super.stop();
        indexer.stop();
        throwableProxyConverter.stop();
        helper.stop();
    }

    @Override
    protected void append(ILoggingEvent event) {
        EsLogRecord record = new EsLogRecord();
        helper.prepareLogRecord(record, event.getTimeStamp());
        record.thread = event.getThreadName();
        record.logger = event.getLoggerName();
        record.message = helper.truncate(event.getFormattedMessage());
        record.level = event.getLevel().levelStr;
        IThrowableProxy throwableProxy = event.getThrowableProxy();
        if (throwableProxy != null) {
            record.exceptionClass = throwableProxy.getClassName();
            record.exceptionMessage = helper.truncate(throwableProxy.getMessage());
            record.stackTrace = helper.truncate(throwableProxyConverter.convert(event));
        }
        record.mdc = new HashMap<>();
        for (Entry<String, String> entry : event.getMDCPropertyMap().entrySet()) {
            record.mdc.put(entry.getKey().replace('.', '_'), helper.truncate(entry.getValue()));
        }
        indexer.queue(helper.getIndex(record.time), esType, record);
    }

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
    }

    @Override
    public EsIndexer getIndexer() {
        return indexer;
    }

    @Override
    public EsAppenderHelper getHelper() {
        return helper;
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
    public void setFailurePause(String failurePause) {
        EsIndexerProps.super.setFailurePause(failurePause);
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
