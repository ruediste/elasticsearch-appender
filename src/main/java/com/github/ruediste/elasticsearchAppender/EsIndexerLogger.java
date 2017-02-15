package com.github.ruediste.elasticsearchAppender;

/**
 * Strategy class allowing the indexer to emit log messages without a dependency
 * on any logging framework
 */
public abstract class EsIndexerLogger {

    abstract public void info(String message);

    abstract public void warn(String message);

    abstract public void error(String message);
}
