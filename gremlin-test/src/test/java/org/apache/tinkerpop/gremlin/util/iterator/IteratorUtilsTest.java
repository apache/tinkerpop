package org.apache.tinkerpop.gremlin.util.iterator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IteratorUtilsTest {
    @Test
    public void shouldIterateSingleObject() {
        assertIterator(IteratorUtils.of("test1"), 1);
    }

    @Test
    public void shouldIteratePairOfObjects() {
        assertIterator(IteratorUtils.of("test1", "test2"), 2);
    }

    @Test
    public void shouldConvertIterableToIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.convertToIterator(iterable), iterable.size());
    }

    @Test
    public void shouldConvertIteratorToIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.convertToIterator(iterable.iterator()), iterable.size());
    }

    @Test
    public void shouldConvertArrayToIterator() {
        final String[] iterable = new String[3];
        iterable[0] = "test1";
        iterable[1] = "test2";
        iterable[2] = "test3";
        assertIterator(IteratorUtils.convertToIterator(iterable), iterable.length);
    }

    @Test
    public void shouldConvertThrowableToIterator() {
        final Exception ex = new Exception("test1");
        assertIterator(IteratorUtils.convertToIterator(ex), 1);
    }

    @Test
    public void shouldConvertStreamToIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.convertToIterator(iterable.stream()), iterable.size());
    }

    @Test
    public void shouldConvertMapToIterator() {
        final Map<String,String> m = new HashMap<>();
        m.put("key1", "val1");
        m.put("key2", "val2");
        m.put("key3", "val3");

        final Iterator itty = IteratorUtils.convertToIterator(m);
        for (int ix = 0; ix < m.size(); ix++) {
            final Map.Entry entry = (Map.Entry) itty.next();
            assertEquals("key" + (ix + 1), entry.getKey());
            assertEquals("val" + (ix + 1), entry.getValue());
        }

        assertFalse(itty.hasNext());
    }

    @Test
    public void shouldConvertAnythingElseToIteratorByWrapping() {
        assertIterator(IteratorUtils.convertToIterator("test1"), 1);
    }

    public <S> void assertIterator(final Iterator<S> itty, final int size) {
        for (int ix = 0; ix < size; ix++) {
            assertEquals("test" + (ix + 1), itty.next());
        }

        assertFalse(itty.hasNext());
    }
}
