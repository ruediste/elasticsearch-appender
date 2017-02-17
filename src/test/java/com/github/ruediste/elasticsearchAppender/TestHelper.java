package com.github.ruediste.elasticsearchAppender;

import java.time.Duration;
import java.time.Instant;

import com.github.ruediste.elasticsearchAppender.EsIndexerEsTest.ThrowingSupplier;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;

public class TestHelper {

    public static void awaitTrue(ThrowingSupplier<Boolean> condition) {
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

    private static JestClient jestClient;

    public static synchronized JestClient getJestClient() {
        if (jestClient == null) {
            jestClient = new JestClientFactory().getObject();
        }
        return jestClient;
    }
}
