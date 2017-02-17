package com.github.ruediste.elasticsearchAppender.log4j;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.ruediste.elasticsearchAppender.TestHelper;

import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

public class EsAppenderLog4j1Test {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSendLogMessage() throws InterruptedException {
        String id = UUID.randomUUID().toString();
        org.apache.log4j.MDC.put("test", "testVal");
        Logger.getLogger(EsAppenderLog4j1Test.class).info("Hello World " + id);
        TestHelper.awaitTrue(() -> {
            SearchResult result = TestHelper.getJestClient()
                    .execute(new Search.Builder("{\"query\":{\"match\":{\"message\":\"" + id + "\"}}}").build());
            return 1 == result.getTotal();
        });
    }
}
