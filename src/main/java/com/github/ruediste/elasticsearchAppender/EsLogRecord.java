package com.github.ruediste.elasticsearchAppender;

import java.util.List;
import java.util.Map;

public class EsLogRecord {
    public long time;
    public Long timeAdjustment;
    public String thread;
    public String logger;
    public String message;
    public String level;
    public String exceptionClass;
    public String exceptionMessage;
    public String stackTrace;
    public Map<String, String> mdc;
    public List<String> ndc;

    public java.util.Set<String> tags;
    public Map<String, String> labels;

    /**
     * Additional properties of the record
     */
    public Map<String, Object> ext;
}