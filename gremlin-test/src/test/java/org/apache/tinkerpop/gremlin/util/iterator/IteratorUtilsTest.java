package org.apache.tinkerpop.gremlin.util.iterator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
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

    @Test
    public void shouldConvertIterableToList() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.convertToList(iterable).iterator(), iterable.size());
    }

    @Test
    public void shouldConvertIteratorToList() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.convertToList(iterable.iterator()).iterator(), iterable.size());
    }

    @Test
    public void shouldConvertArrayToList() {
        final String[] iterable = new String[3];
        iterable[0] = "test1";
        iterable[1] = "test2";
        iterable[2] = "test3";
        assertIterator(IteratorUtils.convertToList(iterable).iterator(), iterable.length);
    }

    @Test
    public void shouldConvertThrowableToList() {
        final Exception ex = new Exception("test1");
        assertIterator(IteratorUtils.convertToList(ex).iterator(), 1);
    }

    @Test
    public void shouldConvertStreamToList() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.convertToList(iterable.stream()).iterator(), iterable.size());
    }

    @Test
    public void shouldConvertMapToList() {
        final Map<String,String> m = new HashMap<>();
        m.put("key1", "val1");
        m.put("key2", "val2");
        m.put("key3", "val3");

        final Iterator itty = IteratorUtils.convertToList(m).iterator();
        for (int ix = 0; ix < m.size(); ix++) {
            final Map.Entry entry = (Map.Entry) itty.next();
            assertEquals("key" + (ix + 1), entry.getKey());
            assertEquals("val" + (ix + 1), entry.getValue());
        }

        assertFalse(itty.hasNext());
    }

    @Test
    public void shouldConvertAnythingElseToListByWrapping() {
        assertIterator(IteratorUtils.convertToList("test1").iterator(), 1);
    }

    @Test
    public void shouldFillFromIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        final List<String> newList = new ArrayList<>();
        IteratorUtils.fill(iterable.iterator(), newList);

        assertIterator(newList.iterator(), iterable.size());
    }

    @Test
    public void shouldCountEmpty() {
        assertEquals(0, IteratorUtils.count(new ArrayList<>().iterator()));
    }

    @Test
    public void shouldCountAll() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        assertEquals(3, IteratorUtils.count(iterable.iterator()));
    }

    @Test
    public void shouldMakeArrayListFromIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.list(iterable.iterator()).iterator(), iterable.size());
    }

    @Test
    public void shouldMatchAllPositively() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertTrue(IteratorUtils.allMatch(iterable.iterator(), s -> s.startsWith("test")));
    }

    @Test
    public void shouldMatchAllNegatively() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertFalse(IteratorUtils.allMatch(iterable.iterator(), s -> s.startsWith("test1")));
    }

    @Test
    public void shouldMatchAnyPositively() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertTrue(IteratorUtils.anyMatch(iterable.iterator(), s -> s.startsWith("test3")));
    }

    @Test
    public void shouldMatchAnyNegatively() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertFalse(IteratorUtils.anyMatch(iterable.iterator(), s -> s.startsWith("dfaa")));
    }

    @Test
    public void shouldMatchNonePositively() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertTrue(IteratorUtils.noneMatch(iterable.iterator(), s -> s.startsWith("test4")));
    }

    @Test
    public void shouldMatchNoneNegatively() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertFalse(IteratorUtils.noneMatch(iterable.iterator(), s -> s.startsWith("test")));
    }

    @Test
    public void shouldProduceMapFromIteratorUsingIdentityForValue() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        final Map<String,String> m = IteratorUtils.collectMap(iterable.iterator(), k -> k.substring(4));
        assertEquals("test1", m.get("1"));
        assertEquals("test2", m.get("2"));
        assertEquals("test3", m.get("3"));
    }

    @Test
    public void shouldProduceMapFromIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        final Map<String,String> m = IteratorUtils.collectMap(iterable.iterator(), k -> k.substring(4), v -> v.substring(0, 4));
        assertEquals("test", m.get("1"));
        assertEquals("test", m.get("2"));
        assertEquals("test", m.get("3"));
    }

    @Test
    public void shouldProduceMapFromIteratorUsingGrouping() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        final Map<String,List<String>> m1 = IteratorUtils.groupBy(iterable.iterator(), i -> i.substring(4));
        assertEquals("test1", m1.get("1").get(0));
        assertEquals(1, m1.get("1").size());
        assertEquals("test2", m1.get("2").get(0));
        assertEquals(1, m1.get("2").size());
        assertEquals("test3", m1.get("3").get(0));
        assertEquals(1, m1.get("3").size());
        assertEquals(3, m1.size());

        final Map<String,List<String>> m2 = IteratorUtils.groupBy(iterable.iterator(), i -> i.substring(0,4));
        assertEquals("test1", m2.get("test").get(0));
        assertEquals("test2", m2.get("test").get(1));
        assertEquals("test3", m2.get("test").get(2));
        assertEquals(3, m2.get("test").size());
        assertEquals(1, m2.size());
    }

    @Test
    public void shouldApplyMapOverIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("1");
        iterable.add("2");
        iterable.add("3");

        assertIterator(IteratorUtils.map(iterable.iterator(), s -> "test" + s), 3);
    }

    @Test
    public void shouldApplyMapOverIterable() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("1");
        iterable.add("2");
        iterable.add("3");

        assertIterator(IteratorUtils.map(iterable, s -> "test" + s).iterator(), 3);
    }

    @Test
    public void shouldFilterAllFromIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        assertIterator(IteratorUtils.filter(iterable.iterator(), s -> s.startsWith("dfaa")), 0);
    }

    @Test
    public void shouldFilterNoneFromIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        assertIterator(IteratorUtils.filter(iterable.iterator(), s -> s.startsWith("test")), 3);
    }

    @Test
    public void shouldFilterSomeFromIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        assertIterator(IteratorUtils.filter(iterable.iterator(), s -> s.equals("test1")), 1);
    }

    @Test
    public void shouldFilterAllFromIterable() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        assertIterator(IteratorUtils.filter(iterable, s -> s.startsWith("dfaa")).iterator(), 0);
    }

    @Test
    public void shouldFilterNoneFromIterable() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        assertIterator(IteratorUtils.filter(iterable, s -> s.startsWith("test")).iterator(), 3);
    }

    @Test
    public void shouldFilterSomeFromIterable() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        assertIterator(IteratorUtils.filter(iterable, s -> s.equals("test1")).iterator(), 1);
    }


    public <S> void assertIterator(final Iterator<S> itty, final int size) {
        for (int ix = 0; ix < size; ix++) {
            assertEquals("test" + (ix + 1), itty.next());
        }

        assertFalse(itty.hasNext());
    }
}
