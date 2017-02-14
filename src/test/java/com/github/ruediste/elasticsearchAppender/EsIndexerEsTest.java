package com.github.ruediste.elasticsearchAppender;

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
        indexer = new EsIndexer("test");
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
