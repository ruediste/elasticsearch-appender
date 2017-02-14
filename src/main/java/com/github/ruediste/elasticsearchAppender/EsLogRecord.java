package com.github.ruediste.elasticsearchAppender;

import java.util.List;
import java.util.Map;

public class EsLogRecord {
    public long timestamp;
    public String threadName;
    public String loggerName;
    public String message;
    public String logLevel;
    public Map<String, String> mdc;
    public List<String> ndc;
    public String exceptionClass;
    public String stackTrace;
}