package com.tinkerpop.gremlin.pipes.util;

import junit.framework.TestCase;

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExpandableIteratorTest extends TestCase {

    public void testExpansion() {
        ExpandableIterator itty = new ExpandableIterator(Arrays.asList().iterator());
        assertFalse(itty.hasNext());
        itty.add(Arrays.asList(0, 1).iterator());
        assertTrue(itty.hasNext());
        itty.add(Arrays.asList(2, 3, 4, 5).iterator());
        assertTrue(itty.hasNext());
        itty.add(Arrays.asList(6, 7, 8, 9).iterator());
        assertTrue(itty.hasNext());
        int counter = 0;
        for (int i = 0; i < 10; i++) {
            counter++;
            assertTrue(itty.hasNext());
            assertEquals(itty.next(), i);
        }
        assertFalse(itty.hasNext());
        assertEquals(counter, 10);
        itty.add(Arrays.asList(0, 1, 2, 3, 4).iterator());
        assertTrue(itty.hasNext());
        for (int i = 0; i < 5; i++) {
            counter++;
            assertTrue(itty.hasNext());
            assertEquals(itty.next(), i);
        }
        assertEquals(counter, 15);
        assertFalse(itty.hasNext());

    }
}
