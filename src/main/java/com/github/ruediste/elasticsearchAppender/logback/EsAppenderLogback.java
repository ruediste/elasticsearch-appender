package com.github.ruediste.elasticsearchAppender.logback;

import java.time.Duration;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ruediste.elasticsearchAppender.EsIndexer;
import com.github.ruediste.elasticsearchAppender.EsIndexerLogger;
import com.github.ruediste.elasticsearchAppender.EsLogRecord;

import ch.qos.logback.classic.pattern.ThrowableProxyConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;

public class EsAppenderLogback extends UnsynchronizedAppenderBase<ILoggingEvent> {

    private EsIndexer indexer = new EsIndexer();
    private ThrowableProxyConverter throwableProxyConverter = new ThrowableProxyConverter();

    @Override
    public void start() {
        super.start();
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
    }

    @Override
    protected void append(ILoggingEvent event) {
        EsLogRecord record = new EsLogRecord();
        indexer.calcNextTimestamp(event.getTimeStamp(), x -> record.time = x, x -> record.timeAdjustment = x);
        record.threadName = event.getThreadName();
        record.loggerName = event.getLoggerName();
        record.message = event.getFormattedMessage();
        record.logLevel = event.getLevel().levelStr;
        record.exceptionClass = event.getThrowableProxy().getClassName();
        record.exceptionMessage = event.getThrowableProxy().getMessage();
        throwableProxyConverter.start();
        record.stackTrace = throwableProxyConverter.convert(event);
        record.mdc = new HashMap<>(event.getMDCPropertyMap());
        indexer.queue("", "", record);
    }

    public int getMaxStringLength() {
        return indexer.maxStringLength;
    }

    public void setMaxStringLength(int maxStringLength) {
        indexer.maxStringLength = maxStringLength;
    }

    public void setThreadName(String threadName) {
        indexer.setThreadName(threadName);
    }

    public void setCapacity(int capacity) {
        indexer.setCapacity(capacity);
    }

    public void setMaxBulkDocumentCount(int maxBulkDocumentCount) {
        indexer.setMaxBulkDocumentCount(maxBulkDocumentCount);
    }

    public void setMaxBulkMemorySize(int maxBulkMemorySize) {
        indexer.setMaxBulkMemorySize(maxBulkMemorySize);
    }

    public void setStopTimeout(Duration stopTimeout) {
        indexer.setStopTimeout(stopTimeout);
    }

    public void setEsUrl(String esUrl) {
        indexer.setEsUrl(esUrl);
    }

    public void setPerformJMXRegistration(boolean performJMXRegistration) {
        indexer.setPerformJMXRegistration(performJMXRegistration);
    }

    public void setmBeanName(String mBeanName) {
        indexer.setmBeanName(mBeanName);
    }

    @Override
    public void setName(String name) {
        indexer.setName(name);
    }

}
