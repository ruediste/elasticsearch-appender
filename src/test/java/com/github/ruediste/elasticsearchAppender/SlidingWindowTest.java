package com.github.ruediste.elasticsearchAppender;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SlidingWindowTest {

    SlidingWindow wnd;
    long time;

    @org.junit.Before
    public void before() {
        wnd = new SlidingWindow(100, 3);
        time = 3457234L;
    }

    @Test
    public void testSingleValue() {
        wnd.addEvent(time);
        time += 150;
        assertEquals(1, wnd.getEventCount(time));
    }

    @Test
    public void testSimple() {
        wnd.addEvent(time);
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);

        time += 100;
        assertEquals(3, wnd.getEventCount(time));
    }

    @Test
    public void testOverflow() {
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);
        time += 100;
        assertEquals(3, wnd.getEventCount(time));
    }

    @Test
    public void testJustFilled() {
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);
        time += 100;
        assertEquals(3, wnd.getEventCount(time));
    }

    @Test
    public void testOverflowWithGap() {
        wnd.addEvent(time);
        time += 200;
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);
        time += 100;
        assertEquals(2, wnd.getEventCount(time));
    }

    @Test
    public void testJump2() {
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);
        time += 200;
        wnd.addEvent(time);
        time += 100;
        assertEquals(2, wnd.getEventCount(time));
    }

    @Test
    public void testJump3() {
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);
        time += 300;
        wnd.addEvent(time);
        time += 100;
        assertEquals(1, wnd.getEventCount(time));
    }

    @Test
    public void testJump4() {
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);
        time += 400;
        wnd.addEvent(time);
        time += 100;
        assertEquals(1, wnd.getEventCount(time));
    }

    @Test
    public void testJump5() {
        wnd.addEvent(time);
        time += 100;
        wnd.addEvent(time);
        time += 500;
        wnd.addEvent(time);
        time += 100;
        assertEquals(1, wnd.getEventCount(time));
    }
}