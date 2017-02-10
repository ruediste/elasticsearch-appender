package com.github.ruediste.elasticsearchAppender;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;
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
        assertArrayEquals(new Object[] { "foo" }, drain(10));
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
        assertArrayEquals(new Object[] { "foo" }, drain(10, 100, Duration.ofSeconds(1)));
        assertTrue(start.plusMillis(400).isBefore(Instant.now()));
    }

    @Test
    public void testTakeTimeout() {
        Instant start = Instant.now();
        assertArrayEquals(new Object[] {}, drain(10, 100, Duration.ofMillis(200)));
        assertTrue(start.plusMillis(150).isBefore(Instant.now()));
    }

    @Test(timeout = 100)
    public void testTakeNoWait() {
        assertArrayEquals(new Object[] {}, drain(10, 100, Duration.ZERO));
    }

    @Test(timeout = 1000)
    public void testTakeTimeoutNull() {
        Thread mainThread = Thread.currentThread();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                mainThread.interrupt();
            }
        }, 100);
        try {
            assertArrayEquals(new Object[] {}, drain(10, 100, null));
        } catch (Exception e) {
            // swallow
        }
    }

    @Test
    public void testDrainSimple() throws Exception {
        put("foo");
        put("bar");
        assertArrayEquals(new Object[] { "foo", "bar" }, drain(10));
    }

    @Test
    public void testDrainMaxCountSmaller() throws Exception {
        put("foo");
        put("bar");
        put("fooBar");
        assertArrayEquals(new Object[] { "foo", "bar" }, drain(2));
        assertArrayEquals(new Object[] { "fooBar" }, drain(2));

    }

    @Test
    public void testDrainMaxCountEqual() throws Exception {
        put("foo");
        put("bar");
        put("fooBar");
        assertArrayEquals(new Object[] { "foo", "bar", "fooBar" }, drain(3));
        assertArrayEquals(new Object[] {}, drain(2));
    }

    @Test
    public void testDrainMaxCountLarger() throws Exception {
        put("foo");
        put("bar");
        put("fooBar");
        assertArrayEquals(new Object[] { "foo", "bar", "fooBar" }, drain(4));
        assertArrayEquals(new Object[] {}, drain(2));
    }

    @Test
    public void testDrainOverflow() throws Exception {
        // 3 bytes times five elements
        buf = new BlockingRingBuffer(3 * 5);
        put(3);
        drain(10);
        put(4);
        assertArrayEquals(new Object[] { "e3", "e4", "e5", "e6" }, drain(10));
    }

    @Test
    public void testDrainLimit() throws Exception {
        // 3 bytes times five elements
        buf = new BlockingRingBuffer(3 * 5);
        put(3);
        drain(10);
        put(2);
        assertArrayEquals(new Object[] { "e3", "e4" }, drain(10));
    }

    @Test
    public void testDrainMaxSize() throws Exception {
        // 20 bytes
        put(10);
        assertEquals(4, drain(100, 9, null).length);
    }

    @Test
    public void testDrainFirstNoLimit() throws Exception {
        // 20 bytes
        put(10);
        assertEquals(1, drain(100, 1, null).length);
    }

    private String[] drain(int maxCount) {
        return drain(maxCount, -1, Duration.ZERO);
    }

    private String[] drain(int maxCount, int maxSize, Duration maxWait) {
        List<byte[]> drain = buf.drain(maxCount, maxSize, maxWait);
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

    @Test
    public void testIntToBytes() throws Exception {
        assertArrayEquals(new byte[] { 0x3 }, buf.intToBytes(3));
        assertArrayEquals(new byte[] { (byte) 0xfe, 0x1 }, buf.intToBytes(0xfe));
        assertArrayEquals(new byte[] { (byte) 0xa3, 0x2 }, buf.intToBytes(0x123));
        // 0001 00 10 0011 0 100 0101
        // 0001 00 |10 0011 0 |100 0101
        assertArrayEquals(new byte[] { (byte) 0xc5, (byte) 0xc6, 0x4, 0x0 }, buf.intToBytes(0x12345));

        // 0x347d8737
        // 0011 0100 0111 1101 1000 0111 0011 0111
        // 1101 0001 1111 0110 |000 1110 |011 0111
        // d1 f6 8e b7
        assertArrayEquals(new byte[] { (byte) 0xb7, (byte) 0x8e, (byte) 0xf6, (byte) 0xd1 },
                buf.intToBytes(0x347d8737));
    }

    @Test
    public void testReadInt() throws Exception {
        checkIntHandling(0x347d8737);
        checkIntHandling(3);
        checkIntHandling(500);
        checkIntHandling(0x123);
    }

    @Test(expected = ArithmeticException.class)
    public void testIntHandlingOverflow() throws Exception {
        checkIntHandling(1 << 30);

    }

    @Test
    public void testIntHandlingRandom() throws Exception {
        Random r = new Random(1);
        for (long i = 0; i < 1000000; i++) {
            checkIntHandling(r.nextInt(1 << 30));
        }
    }

    private void checkIntHandling(int value) {
        buf.setBufferContents(buf.intToBytes(value));
        assertEquals(value, buf.readInt());
    }
}
