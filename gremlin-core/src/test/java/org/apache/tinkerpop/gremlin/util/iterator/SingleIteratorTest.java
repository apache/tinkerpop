package org.apache.tinkerpop.gremlin.util.iterator;

import org.apache.tinkerpop.gremlin.process.FastNoSuchElementException;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SingleIteratorTest {
    @Test
    public void shouldIterateAnArray() {
        final Iterator<String> itty = new SingleIterator<>("test1");
        assertEquals("test1", itty.next());

        assertFalse(itty.hasNext());
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowFastNoSuchElementException() {
        final Iterator<String> itty = new SingleIterator<>("test1");
        itty.next();
        itty.next();
    }
}
