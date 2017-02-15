package com.github.ruediste.elasticsearchAppender;

import java.time.Instant;

public class EsIndexerLoggerConsole extends EsIndexerLogger {

    @Override
    public void info(String message) {
        System.out.println(Instant.now() + " INFO:  " + message);

    }

    @Override
    public void warn(String message) {
        System.err.println(Instant.now() + " WARN:  " + message);
    }

    @Override
    public void error(String message) {
        System.err.println(Instant.now() + " ERROR: " + message);
    }

}
