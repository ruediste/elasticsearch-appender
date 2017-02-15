package com.github.ruediste.elasticsearchAppender;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

public class EsIndexerEsTest {
    Random random = new Random();
    EsIndexer indexer;

    @Before
    public void setUp() throws Exception {
        indexer = new EsIndexer("test", new EsIndexerLoggerConsole());
        indexer.start();
    }

    @After
    public void tearDown() throws Exception {
        indexer.stop();
    }

    @Test
    public void testSendRequest() {
        long id = random.nextLong();
        indexer.queue("test", "test", "{\"id\":\"" + id + "\"}");
        awaitTrue(() -> {
            SearchResult result = indexer.jestClient
                    .execute(new Search.Builder("{\"query\":{\"term\":{\"id\":\"" + id + "\"}}}").build());
            return 1 == result.getTotal();
        });
    }

    @Test
    public void testSendFailingRequest() throws InterruptedException {
        indexer.queue("test", "test", "{malformed json}");
        awaitTrue(() -> indexer.getTotalEventIndexingFailedCount() > 0);
    }

    @Test
    public void perfTest() throws InterruptedException {
        Instant start = Instant.now();
        Instant end = start.plus(Duration.ofSeconds(10));
        while (Instant.now().isBefore(end)) {
            if (indexer.getQueueFillFraction() > 0.8)
                Thread.sleep(10);
            else
                for (int i = 0; i < 10; i++)
                    indexer.queue("test", "test", "{}");
        }
        System.out.println("ES indexing performance: "
                + (1000.0 * indexer.getTotalEventIndexedCount() / Duration.between(start, Instant.now()).toMillis())
                + "/s");
        assertEquals(0, indexer.getTotalEventDiscardedCount());
        assertEquals(0, indexer.getTotalEventIndexingFailedCount());
    }

    private interface ThrowingSupplier<T> {
        T get() throws Throwable;
    }

    private void awaitTrue(ThrowingSupplier<Boolean> condition) {
        Instant end = Instant.now().plus(Duration.ofSeconds(5));
        Throwable lastError = null;
        while (Instant.now().isBefore(end)) {
            try {
                if (condition.get())
                    return;
            } catch (Throwable t) {
                lastError = t;
            }
            try {
                Thread.sleep(100);
            } catch (Throwable t) {
                // swallow
            }
        }
        throw new RuntimeException("Timout reached while waiting for condiditon to become true", lastError);
    }
}
