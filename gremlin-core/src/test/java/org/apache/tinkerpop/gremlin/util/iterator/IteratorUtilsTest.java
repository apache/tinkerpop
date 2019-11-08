/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util.iterator;

import org.apache.tinkerpop.gremlin.AssertHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IteratorUtilsTest {

    @Test
    public void shouldBeUtilityClass() throws Exception {
        AssertHelper.assertIsUtilityClass(IteratorUtils.class);
    }

    @Test
    public void shouldIterateSingleObject() {
        assertIterator(IteratorUtils.of("test1"), 1);
    }

    @Test
    public void shouldIteratePairOfObjects() {
        assertIterator(IteratorUtils.of("test1", "test2"), 2);
    }

    @Test
    public void shouldIterateToEnd() {
        final Iterator itty = Arrays.asList(1, 2, 3).iterator();
        IteratorUtils.iterate(itty);
        assertThat(itty.hasNext(), is(false));
    }

    @Test
    public void shouldNotFailToIterateIfIteratorIsEmpty() {
        final Iterator itty = new ArrayList().iterator();
        IteratorUtils.iterate(itty);
        assertThat(itty.hasNext(), is(false));
    }

    @Test
    public void shouldConvertIteratorToListAndSort() {
        final Iterator<Integer> itty = Arrays.asList(1, 2, 3).iterator();
        final List<Integer> list = IteratorUtils.list(itty, Comparator.<Integer>reverseOrder());
        assertEquals(3, list.get(0).intValue());
        assertEquals(2, list.get(1).intValue());
        assertEquals(1, list.get(2).intValue());
    }

    @Test
    public void shouldIterateToSet() {
        final List<Integer> list = Arrays.asList(1, 2, 3, 3, 2, 1);
        final Iterator<Integer> itty = list.iterator();
        final Set<Integer> set = IteratorUtils.set(itty);
        assertEquals(3, set.size());
        assertThat(set, is(new HashSet<>(list)));
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowOnLimitIfIteratorExceeded() {
        final Iterator<Integer> itty = Arrays.asList(1, 2, 3).iterator();
        final Iterator<Integer> limitedItty = IteratorUtils.limit(itty, 1);
        limitedItty.next();
        limitedItty.next();
    }

    @Test
    public void shouldFlatMapIterator() {
        final List<Iterator<Integer>> firstList = Arrays.asList(Arrays.asList(1, 2, 3).iterator(), new ArrayList<Integer>().iterator(), Arrays.asList(4, 5, 6).iterator());
        final Iterator<Iterator<Integer>> itty = firstList.iterator();
        final List<Integer> list = IteratorUtils.list(IteratorUtils.flatMap(itty, x -> IteratorUtils.map(x, i -> i * 10)));
        assertEquals(6, list.size());
        assertThat(list, is(Arrays.asList(10, 20, 30, 40, 50, 60)));
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowOnFlatMapIfIteratorExceeded() {
        final Iterator<Iterator<Integer>> itty = Arrays.asList(Arrays.asList(1).iterator(), new ArrayList<Integer>().iterator(), Arrays.asList(4).iterator()).iterator();
        final Iterator<Integer> limitedItty = IteratorUtils.flatMap(itty, x -> IteratorUtils.map(x, i -> i * 10));
        limitedItty.next();
        limitedItty.next();
        limitedItty.next();
    }

    @Test
    public void shouldConvertIteratorToStream() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        assertEquals(3, IteratorUtils.stream(iterable.iterator()).count());
        final Object[] o = IteratorUtils.stream(iterable.iterator()).toArray();
        assertEquals("test1", o[0]);
        assertEquals("test2", o[1]);
        assertEquals("test3", o[2]);
    }

    @Test
    public void shouldConvertIterableToIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.asIterator(iterable), iterable.size());
    }

    @Test
    public void shouldConvertIteratorToIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.asIterator(iterable.iterator()), iterable.size());
    }

    @Test
    public void shouldConvertArrayToIterator() {
        final String[] iterable = new String[3];
        iterable[0] = "test1";
        iterable[1] = "test2";
        iterable[2] = "test3";
        assertIterator(IteratorUtils.asIterator(iterable), iterable.length);
    }

    @Test
    public void shouldConvertThrowableToIterator() {
        final Exception ex = new Exception("test1");
        assertIterator(IteratorUtils.asIterator(ex), 1);
    }

    @Test
    public void shouldConvertStreamToIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.asIterator(iterable.stream()), iterable.size());
    }

    @Test
    public void shouldConvertMapToIterator() {
        final Map<String,String> m = new HashMap<>();
        m.put("key1", "val1");
        m.put("key2", "val2");
        m.put("key3", "val3");

        final Iterator itty = IteratorUtils.asIterator(m);
        for (int ix = 0; ix < m.size(); ix++) {
            final Map.Entry entry = (Map.Entry) itty.next();
            assertEquals("key" + (ix + 1), entry.getKey());
            assertEquals("val" + (ix + 1), entry.getValue());
        }

        assertFalse(itty.hasNext());
    }

    @Test
    public void shouldConvertAnythingElseToIteratorByWrapping() {
        assertIterator(IteratorUtils.asIterator("test1"), 1);
    }

    @Test
    public void shouldConvertIterableToList() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.asList(iterable).iterator(), iterable.size());
    }

    @Test
    public void shouldConvertIteratorToList() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.asList(iterable.iterator()).iterator(), iterable.size());
    }

    @Test
    public void shouldConvertArrayToList() {
        final String[] iterable = new String[3];
        iterable[0] = "test1";
        iterable[1] = "test2";
        iterable[2] = "test3";
        assertIterator(IteratorUtils.asList(iterable).iterator(), iterable.length);
    }

    @Test
    public void shouldConvertThrowableToList() {
        final Exception ex = new Exception("test1");
        assertIterator(IteratorUtils.asList(ex).iterator(), 1);
    }

    @Test
    public void shouldConvertStreamToList() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");
        assertIterator(IteratorUtils.asList(iterable.stream()).iterator(), iterable.size());
    }

    @Test
    public void shouldConvertMapToList() {
        final Map<String,String> m = new HashMap<>();
        m.put("key1", "val1");
        m.put("key2", "val2");
        m.put("key3", "val3");

        final Iterator itty = IteratorUtils.asList(m).iterator();
        for (int ix = 0; ix < m.size(); ix++) {
            final Map.Entry entry = (Map.Entry) itty.next();
            assertEquals("key" + (ix + 1), entry.getKey());
            assertEquals("val" + (ix + 1), entry.getValue());
        }

        assertFalse(itty.hasNext());
    }

    @Test
    public void shouldConvertAnythingElseToListByWrapping() {
        assertIterator(IteratorUtils.asList("test1").iterator(), 1);
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

        final Map<String,List<String>> m2 = IteratorUtils.groupBy(iterable.iterator(), i -> i.substring(0, 4));
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

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowOnFilterIfNextedPastAvailable() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        final Iterator<String> filteredItty = IteratorUtils.filter(iterable.iterator(), s -> s.startsWith("dfaa"));
        filteredItty.next();
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
    public void shouldFilterNoneFromIteratorWithoutHasNextCheckFirst() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        final Iterator<String> filteredItty = IteratorUtils.filter(iterable.iterator(), s -> s.startsWith("test"));
        assertEquals("test1",  filteredItty.next());
        assertEquals("test2",  filteredItty.next());
        assertEquals("test3",  filteredItty.next());
    }

    @Test
    public void shouldFilterNoneFromIteratorWithMultipleChecksOfHasNext() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("test1");
        iterable.add("test2");
        iterable.add("test3");

        final Iterator<String> filteredItty = IteratorUtils.filter(iterable.iterator(), s -> s.startsWith("test"));
        assertThat(filteredItty.hasNext(), is(true));
        assertThat(filteredItty.hasNext(), is(true));
        assertThat(filteredItty.hasNext(), is(true));
        assertIterator(filteredItty, 3);
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

    @Test
    public void shouldConcatIterators() {
        final List<String> iterable1 = new ArrayList<>();
        iterable1.add("test1");
        iterable1.add("test2");
        iterable1.add("test3");

        final List<String> iterable2 = new ArrayList<>();
        iterable2.add("test4");
        iterable2.add("test5");
        iterable2.add("test6");

        assertIterator(IteratorUtils.concat(iterable1.iterator(), iterable2.iterator()), 6);
    }

    @Test
    public void shouldReduceFromIteratorWithBinaryOperator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("1");
        iterable.add("2");
        iterable.add("3");

        assertEquals("test123", IteratorUtils.reduce(iterable.iterator(), "test", (a, b) -> a + b));
    }

    @Test
    public void shouldReduceFromIterableWithBinaryOperator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("1");
        iterable.add("2");
        iterable.add("3");

        assertEquals("test123", IteratorUtils.reduce(iterable, "test", (a, b) -> a + b));
    }

    @Test
    public void shouldReduceFromIterator() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("1");
        iterable.add("2");
        iterable.add("3");

        assertEquals(new Integer(16), IteratorUtils.reduce(iterable.iterator(), 10, (accumulator, val) -> accumulator + Integer.parseInt(val)));
    }

    @Test
    public void shouldReduceFromIterable() {
        final List<String> iterable = new ArrayList<>();
        iterable.add("1");
        iterable.add("2");
        iterable.add("3");

        assertEquals(new Integer(16), IteratorUtils.reduce(iterable, 10, (accumulator, val) -> accumulator + Integer.parseInt(val)));
    }

    @Test
    public void shouldLimitIterator() {
        List<String> list = Arrays.asList("a","b","c","d","e");
        Iterator<String> itty = IteratorUtils.limit(list.iterator(),3);
        assertTrue(itty.hasNext());
        assertEquals("a", itty.next());
        assertTrue(itty.hasNext());
        assertEquals("b", itty.next());
        assertTrue(itty.hasNext());
        assertEquals("c", itty.next());
        assertFalse(itty.hasNext());

        list = Arrays.asList("a","b","c");
        itty = IteratorUtils.limit(list.iterator(),4);
        assertTrue(itty.hasNext());
        assertEquals("a", itty.next());
        assertTrue(itty.hasNext());
        assertEquals("b", itty.next());
        assertTrue(itty.hasNext());
        assertEquals("c", itty.next());
        assertFalse(itty.hasNext());
    }

    public <S> void assertIterator(final Iterator<S> itty, final int size) {
        for (int ix = 0; ix < size; ix++) {
            assertThat(itty.hasNext(), is(true));
            assertEquals("test" + (ix + 1), itty.next());
        }

        assertFalse(itty.hasNext());
    }
}
