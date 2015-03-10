package org.apache.tinkerpop.gremlin.util.iterator;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IteratorUtilsTest {
    @Test
    public void shouldIterateSingleObject() {
        final Iterator<String> itty = IteratorUtils.of("test");
        assertEquals("test", itty.next());
        assertFalse(itty.hasNext());
    }

    @Test
    public void shouldIteratePairOfObjects() {
        final Iterator<String> itty = IteratorUtils.of("test1", "test2");
        assertEquals("test1", itty.next());
        assertEquals("test2", itty.next());
        assertFalse(itty.hasNext());
    }
}
