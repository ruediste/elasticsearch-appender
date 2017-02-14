package com.github.ruediste.elasticsearchAppender;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EsIndexerTest {

    EsIndexer indexer;
    List<EsIndexRequest> processedRequests;

    @Before
    public void setUp() throws Exception {
        processedRequests = new ArrayList<>();
        indexer = new EsIndexer("test") {
            @Override
            protected void processElements(List<byte[]> elements) {
                elements.forEach(bb -> processedRequests.add(toIndexRequest(bb)));
            }
        };
        // indexer.jestClient = mock(JestClient.class);
        indexer.createBuffer();
    }

    @After
    public void tearDown() throws Exception {
        indexer.stop();
    }

    @Test
    public void testIndexRequestPassing() {
        indexer.queue("foo", "bar", "fooBar");
        byte[] bb = indexer.buffer.drain(100, -1, null).get(0);

        EsIndexRequest req = indexer.toIndexRequest(bb);
        assertEquals("foo", req.index);
        assertEquals("bar", req.type);
        assertEquals("fooBar", req.payload);
    }

    @Test(timeout = 3000)
    public void testStartStop() {
        indexer.start();
        indexer.stop();
    }

    @Test
    public void testRequestSending() throws Throwable {
        indexer.start();
        indexer.queue("foo", "bar", "fooBar");
        Thread.sleep(100);
        assertEquals(1, processedRequests.size());
        assertEquals("foo", processedRequests.get(0).index);
    }
}
