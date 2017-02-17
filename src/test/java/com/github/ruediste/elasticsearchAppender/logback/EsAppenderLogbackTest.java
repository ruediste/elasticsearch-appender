package com.github.ruediste.elasticsearchAppender.logback;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.github.ruediste.elasticsearchAppender.TestHelper;

import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

public class EsAppenderLogbackTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSendLogMessage() throws InterruptedException {
        String id = UUID.randomUUID().toString();
        MDC.put("test", "testVal");
        LoggerFactory.getLogger(EsAppenderLogbackTest.class).info("Hello World " + id);
        TestHelper.awaitTrue(() -> {
            SearchResult result = TestHelper.getJestClient()
                    .execute(new Search.Builder("{\"query\":{\"match\":{\"message\":\"" + id + "\"}}}").build());
            return 1 == result.getTotal();
        });
    }

    @Test
    @Ignore
    public void testPerf() throws InterruptedException, IOException {
        Logger log = LoggerFactory.getLogger(EsAppenderLogbackTest.class);
        Instant start = Instant.now();
        Instant end = start.plusSeconds(10);

        while (Instant.now().isBefore(end)) {
            for (int i = 0; i < 100; i++)
                log.info("Hello World ");
        }
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        EsAppenderLogback appender = (EsAppenderLogback) root.getAppender("ES");
        long count = appender.indexer.getTotalEventIndexedCount();
        System.out.println("Perfomance: " + 1000.0 * count / Duration.between(start, Instant.now()).toMillis() + "/s");
        System.in.read();
    }
}
