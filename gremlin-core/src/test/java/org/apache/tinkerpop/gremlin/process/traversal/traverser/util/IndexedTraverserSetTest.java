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
package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IndexedTraverserSetTest {
    @Test
    public void shouldIndexTraversers() {
        final IndexedTraverserSet<String, String> ts = makeTraverserSet();

        final List<Traverser.Admin<String>> testTraversers = new ArrayList<>(ts.get("test"));
        assertEquals(1, testTraversers.size());
        assertEquals(10, testTraversers.get(0).bulk());

        final List<Traverser.Admin<String>> nopeTraversers = new ArrayList<>(ts.get("nope"));
        assertEquals(1, nopeTraversers.size());
        assertEquals(1, nopeTraversers.get(0).bulk());
    }

    @Test
    public void shouldClear() {
        final IndexedTraverserSet<String,String> ts = new IndexedTraverserSet<>(x -> x);
        ts.add(makeTraverser("test", 1));

        final List<Traverser.Admin<String>> testTraversers = new ArrayList<>(ts.get("test"));
        assertEquals(1, testTraversers.size());
        assertEquals(1, testTraversers.get(0).bulk());

        ts.clear();

        assertThat(ts.get("test"), nullValue());
        assertThat(ts.isEmpty(), is(true));
    }

    @Test
    public void shouldRemove() {
        final IndexedTraverserSet<String, String> ts = makeTraverserSet();

        final List<Traverser.Admin<String>> testTraversers = new ArrayList<>(ts.get("test"));
        assertEquals(1, testTraversers.size());
        assertEquals(10, testTraversers.get(0).bulk());

        ts.remove();

        assertThat(ts.get("test"), nullValue());

        final List<Traverser.Admin<String>> nopeTraversers = new ArrayList<>(ts.get("nope"));
        assertEquals(1, nopeTraversers.size());
        assertEquals(1, nopeTraversers.get(0).bulk());
    }

    @Test
    public void shouldRemoveSpecific() {
        final IndexedTraverserSet<String, String> ts = makeTraverserSet();

        final List<Traverser.Admin<String>> testTraversers = new ArrayList<>(ts.get("test"));
        assertEquals(1, testTraversers.size());
        assertEquals(10, testTraversers.get(0).bulk());

        ts.remove(makeTraverser("test", 1));

        assertThat(ts.get("test"), nullValue());

        final List<Traverser.Admin<String>> nopeTraversers = new ArrayList<>(ts.get("nope"));
        assertEquals(1, nopeTraversers.size());
        assertEquals(1, nopeTraversers.get(0).bulk());
    }

    @Test
    public void shouldMaintainIndexOfTraversersAfterShuffle() {
        final IndexedTraverserSet<String, String> ts = makeOtherTraverserSet();

        final List<Traverser.Admin<String>> testTraversers = new ArrayList<>(ts.get("test"));
        assertEquals(1, testTraversers.size());
        assertEquals(4, testTraversers.get(0).bulk());

        final List<Traverser.Admin<String>> nopeTraversers = new ArrayList<>(ts.get("nope"));
        assertEquals(1, nopeTraversers.size());
        assertEquals(1, nopeTraversers.get(0).bulk());

        ts.shuffle();

        final List<Traverser.Admin<String>> testTraversersAfterShuffle = new ArrayList<>(ts.get("test"));
        assertEquals(1, testTraversersAfterShuffle.size());
        assertEquals(4, testTraversersAfterShuffle.get(0).bulk());

        final List<Traverser.Admin<String>> nopeTraversersAfterShuffle = new ArrayList<>(ts.get("nope"));
        assertEquals(1, nopeTraversersAfterShuffle.size());
        assertEquals(1, nopeTraversersAfterShuffle.get(0).bulk());
    }

    @Test
    public void shouldMaintainIndexOfTraversersAfterSort() {
        final IndexedTraverserSet<String, String> ts = makeOtherTraverserSet();

        final List<Traverser.Admin<String>> testTraversers = new ArrayList<>(ts.get("test"));
        assertEquals(1, testTraversers.size());
        assertEquals(4, testTraversers.get(0).bulk());

        final List<Traverser.Admin<String>> nopeTraversers = new ArrayList<>(ts.get("nope"));
        assertEquals(1, nopeTraversers.size());
        assertEquals(1, nopeTraversers.get(0).bulk());

        final List<Traverser<String>> traversersBefore = IteratorUtils.asList(ts.iterator());

        ts.sort(Comparator.comparing(Traverser::get));

        final List<Traverser<String>> traversersAfter = IteratorUtils.asList(ts.iterator());

        final List<Traverser.Admin<String>> testTraversersAfterSort = new ArrayList<>(ts.get("test"));
        assertEquals(1, testTraversersAfterSort.size());
        assertEquals(4, testTraversersAfterSort.get(0).bulk());

        final List<Traverser.Admin<String>> nopeTraversersAfterSort = new ArrayList<>(ts.get("nope"));
        assertEquals(1, nopeTraversersAfterSort.size());
        assertEquals(1, nopeTraversersAfterSort.get(0).bulk());

        assertNotEquals(traversersBefore, traversersAfter);
    }

    private IndexedTraverserSet<String, String> makeTraverserSet() {
        final IndexedTraverserSet<String,String> ts = new IndexedTraverserSet<>(x -> x);
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("nope", 1));
        return ts;
    }

    private IndexedTraverserSet<String, String> makeOtherTraverserSet() {
        final IndexedTraverserSet<String,String> ts = new IndexedTraverserSet<>(x -> x);
        ts.add(makeTraverser("testf", 1));
        ts.add(makeTraverser("teste", 1));
        ts.add(makeTraverser("testd", 1));
        ts.add(makeTraverser("testc", 1));
        ts.add(makeTraverser("testa", 1));
        ts.add(makeTraverser("testb", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("test", 1));
        ts.add(makeTraverser("nope", 1));
        return ts;
    }

    private <T> Traverser.Admin<T> makeTraverser(final T val, final long bulk) {
        return new B_O_Traverser<>(val, bulk).asAdmin();
    }
}
