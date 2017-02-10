package com.github.ruediste.elasticsearchAppender;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Timer;
import java.util.TimerTask;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlockingRingBufferTest {

    Timer timer;
    BlockingRingBuffer buf;
    int putCounter;

    @Before
    public void setUp() throws Exception {
        timer = new Timer(true);
        buf = new BlockingRingBuffer(5);
        putCounter = 0;
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testPutTake() {
        buf.put("foo");
        assertEquals("foo", buf.take());
    }

    @Test
    public void testTakePut() {
        Instant start = Instant.now();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                buf.put("foo");
            }
        }, 500);
        assertEquals("foo", buf.take());
        assertTrue(start.plusMillis(400).isBefore(Instant.now()));
    }

    @Test
    public void testDrainSimple() throws Exception {
        buf.put("foo");
        buf.put("bar");
        assertArrayEquals(new Object[] { "foo", "bar" }, buf.drain(10, Duration.ofSeconds(1)));
    }

    @Test
    public void testDrainMaxCountSmaller() throws Exception {
        buf.put("foo");
        buf.put("bar");
        buf.put("fooBar");
        assertArrayEquals(new Object[] { "foo", "bar" }, buf.drain(2, Duration.ofSeconds(1)));
        assertArrayEquals(new Object[] { "fooBar" }, buf.drain(2, Duration.ofSeconds(1)));

    }

    @Test
    public void testDrainMaxCountEqual() throws Exception {
        buf.put("foo");
        buf.put("bar");
        buf.put("fooBar");
        assertArrayEquals(new Object[] { "foo", "bar", "fooBar" }, buf.drain(3, Duration.ofSeconds(1)));
        assertArrayEquals(new Object[] {}, buf.drain(2, Duration.ZERO));
    }

    @Test
    public void testDrainMaxCountLarger() throws Exception {
        buf.put("foo");
        buf.put("bar");
        buf.put("fooBar");
        assertArrayEquals(new Object[] { "foo", "bar", "fooBar" }, buf.drain(4, Duration.ofSeconds(1)));
        assertArrayEquals(new Object[] {}, buf.drain(2, Duration.ZERO));
    }

    @Test
    public void testDrainOverflow() throws Exception {
        put(3);
        buf.drain(10, null);
        put(4);
        assertArrayEquals(new Object[] { "e3", "e4", "e5", "e6" }, buf.drain(10, null));
    }

    @Test
    public void testDrainLimit() throws Exception {
        put(3);
        buf.drain(10, null);
        put(2);
        assertArrayEquals(new Object[] { "e3", "e4" }, buf.drain(10, null));
    }

    private void put(int count) {
        for (int i = 0; i < count; i++) {
            buf.put("e" + putCounter++);
        }
    }
}
