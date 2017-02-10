package com.github.ruediste.elasticsearchAppender;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlockingRingBufferTest {

    Timer timer;
    BlockingRingBuffer buf;
    int putCounter;

    Charset utf8 = Charset.forName("UTF-8");

    @Before
    public void setUp() throws Exception {
        timer = new Timer(true);
        buf = new BlockingRingBuffer(100);
        putCounter = 0;
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testPutTake() {
        put("foo");
        assertArrayEquals(new Object[] { "foo" }, drain(10, null));
    }

    @Test
    public void testTakePut() {
        Instant start = Instant.now();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                put("foo");
            }
        }, 500);
        assertArrayEquals(new Object[] { "foo" }, drain(10, null));
        assertTrue(start.plusMillis(400).isBefore(Instant.now()));
    }

    @Test
    public void testDrainSimple() throws Exception {
        put("foo");
        put("bar");
        assertArrayEquals(new Object[] { "foo", "bar" }, drain(10, Duration.ofSeconds(1)));
    }

    @Test
    public void testDrainMaxCountSmaller() throws Exception {
        put("foo");
        put("bar");
        put("fooBar");
        assertArrayEquals(new Object[] { "foo", "bar" }, drain(2, Duration.ofSeconds(1)));
        assertArrayEquals(new Object[] { "fooBar" }, drain(2, Duration.ofSeconds(1)));

    }

    @Test
    public void testDrainMaxCountEqual() throws Exception {
        put("foo");
        put("bar");
        put("fooBar");
        assertArrayEquals(new Object[] { "foo", "bar", "fooBar" }, drain(3, Duration.ofSeconds(1)));
        assertArrayEquals(new Object[] {}, drain(2, Duration.ZERO));
    }

    @Test
    public void testDrainMaxCountLarger() throws Exception {
        put("foo");
        put("bar");
        put("fooBar");
        assertArrayEquals(new Object[] { "foo", "bar", "fooBar" }, drain(4, Duration.ofSeconds(1)));
        assertArrayEquals(new Object[] {}, drain(2, Duration.ZERO));
    }

    @Test
    public void testDrainOverflow() throws Exception {
        // 3 bytes times five elements
        buf = new BlockingRingBuffer(3 * 5);
        put(3);
        drain(10, null);
        put(4);
        assertArrayEquals(new Object[] { "e3", "e4", "e5", "e6" }, drain(10, null));
    }

    @Test
    public void testDrainLimit() throws Exception {
        // 3 bytes times five elements
        buf = new BlockingRingBuffer(3 * 5);
        put(3);
        drain(10, null);
        put(2);
        assertArrayEquals(new Object[] { "e3", "e4" }, drain(10, null));
    }

    private String[] drain(int maxCound, Duration maxWait) {
        List<byte[]> drain = buf.drain(maxCound, maxWait);
        return drain.stream().map(x -> new String(x, utf8)).collect(toList()).toArray(new String[] {});
    }

    private void put(String element) {
        buf.put(element.getBytes(utf8));
    }

    private void put(int count) {
        for (int i = 0; i < count; i++) {
            put("e" + putCounter++);
        }
    }
}
