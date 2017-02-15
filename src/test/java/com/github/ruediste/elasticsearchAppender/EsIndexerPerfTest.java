package com.github.ruediste.elasticsearchAppender;

import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import io.searchbox.client.JestClient;

public class EsIndexerPerfTest {

    long processed;
    long expectedCount;
    CountDownLatch allProcessed;

    @Test
    public void testThroughput() throws Throwable {
        processed = 0;
        allProcessed = new CountDownLatch(1);
        expectedCount = 1000000;
        EsIndexer indexer = new EsIndexer("test", new EsIndexerLoggerConsole()) {
            @Override
            protected void processElements(List<byte[]> elements) {
                processed += elements.size();
                if (processed >= expectedCount)
                    allProcessed.countDown();
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        indexer.jestClient = mock(JestClient.class);
        indexer.start();

        Instant startTime = Instant.now();

        for (int i = 0; i < expectedCount; i++)
            indexer.queue("foo", "bar", "fooBar");

        allProcessed.await();
        System.out.println(
                "Throughput: " + 1000.0 * expectedCount / Duration.between(startTime, Instant.now()).toMillis() + "/s");
    }
}
