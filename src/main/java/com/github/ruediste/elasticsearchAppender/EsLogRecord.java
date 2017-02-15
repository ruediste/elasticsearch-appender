package com.github.ruediste.elasticsearchAppender;

import java.util.List;
import java.util.Map;

public class EsLogRecord {
    public long time;
    public long timeAdjustment;
    public String threadName;
    public String loggerName;
    public String message;
    public String logLevel;
    public String exceptionClass;
    public String exceptionMessage;
    public String stackTrace;
    public Map<String, String> mdc;
    public List<String> ndc;

    /**
     * Additional properties of the record
     */
    public Map<String, Object> ext;
}