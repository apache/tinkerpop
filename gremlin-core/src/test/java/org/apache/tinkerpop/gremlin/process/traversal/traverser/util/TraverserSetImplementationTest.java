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
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class TraverserSetImplementationTest {
    @Parameterized.Parameters(name = "expect({0})")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TraverserSet.class.getSimpleName(), (Supplier) TraverserSet::new},
                {IndexedTraverserSet.class.getSimpleName(), (Supplier) () -> new IndexedTraverserSet<String,String>(x -> x.substring(0,1))}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Supplier<TraverserSet> traverserSetMaker;

    @Test
    public void shouldBeEmpty() {
        assertThat(traverserSetMaker.get().isEmpty(), is(true));
    }

    @Test
    public void shouldNotBeEmpty() {
        final TraverserSet<String> ts = traverserSetMaker.get();
        ts.add(makeTraverser("test", 1));
        assertThat(ts.isEmpty(), is(false));
    }

    @Test
    public void shouldGetSize() {
        final TraverserSet<String> ts = traverserSetMaker.get();
        ts.add(makeTraverser("a", 1));
        ts.add(makeTraverser("a", 1));
        ts.add(makeTraverser("b1", 1));
        ts.add(makeTraverser("b2", 2));
        ts.add(makeTraverser("c", 1));

        assertEquals(4, ts.size());
    }

    @Test
    public void shouldGetBulkSize() {
        final TraverserSet<String> ts = makeStringTraversers();

        assertEquals(5, ts.bulkSize());
    }

    @Test
    public void shouldIterateAll() {
        final TraverserSet<String> ts = makeStringTraversers();

        final Iterator<Traverser.Admin<String>> itty = ts.iterator();
        final Traverser.Admin<String> a = itty.next();
        assertEquals("a", a.get());
        assertEquals(2, a.bulk());

        final Traverser.Admin<String> b1 = itty.next();
        assertEquals("b1", b1.get());
        assertEquals(1, b1.bulk());

        final Traverser.Admin<String> b2 = itty.next();
        assertEquals("b2", b2.get());
        assertEquals(1, b2.bulk());

        final Traverser.Admin<String> c = itty.next();
        assertEquals("c", c.get());
        assertEquals(1, c.bulk());

        assertThat(itty.hasNext(), is(false));
    }

    @Test
    public void shouldGetTraverser() {
        final TraverserSet<String> ts = makeStringTraversers();

        final Traverser.Admin<String> a = ts.get(makeTraverser("a", 1));
        assertEquals("a", a.get());
        assertEquals(2, a.bulk());

        final Traverser.Admin<String> b2 = ts.get(makeTraverser("b2", 1));
        assertEquals("b2", b2.get());
        assertEquals(1, b2.bulk());

        final Traverser.Admin<String> notHere = ts.get(makeTraverser("notHere", 1));
        assertNull(notHere);
    }

    @Test
    public void shouldDetermineIfTraverserIsPresent() {
        final TraverserSet<String> ts = makeStringTraversers();

        assertThat(ts.contains(makeTraverser("a", 1)), is(true));
        assertThat(ts.contains(makeTraverser("b1", 1)), is(true));
        assertThat(ts.contains(makeTraverser("b2", 1)), is(true));
        assertThat(ts.contains(makeTraverser("c", 1)), is(true));
        assertThat(ts.contains(makeTraverser("notHere", 1)), is(false));
    }

    @Test
    public void shouldAddAndMerge() {
        final TraverserSet<String> ts = traverserSetMaker.get();
        assertThat(ts.add(makeTraverser("a", 1)), is(true));
        assertThat(ts.add(makeTraverser("a", 1)), is(false));
        assertThat(ts.add(makeTraverser("a", 1)), is(false));
        assertThat(ts.add(makeTraverser("a", 1)), is(false));
        assertThat(ts.add(makeTraverser("a", 1)), is(false));
        assertThat(ts.add(makeTraverser("a", 1)), is(false));
        assertThat(ts.add(makeTraverser("a", 1)), is(false));
        assertThat(ts.add(makeTraverser("b", 1)), is(true));

        final Iterator<Traverser.Admin<String>> itty = ts.iterator();
        final Traverser.Admin<String> a = itty.next();
        assertEquals(7, a.bulk());
        final Traverser.Admin<String> b = itty.next();
        assertEquals(1, b.bulk());

        assertThat(itty.hasNext(), is(false));
    }

    @Test
    public void shouldOfferTraverser() {
        final TraverserSet<String> ts = traverserSetMaker.get();
        assertThat(ts.offer(makeTraverser("a", 1)), is(true));
        assertThat(ts.offer(makeTraverser("a", 1)), is(false));
        assertThat(ts.offer(makeTraverser("a", 1)), is(false));
        assertThat(ts.offer(makeTraverser("a", 1)), is(false));
        assertThat(ts.offer(makeTraverser("a", 1)), is(false));
        assertThat(ts.offer(makeTraverser("a", 1)), is(false));
        assertThat(ts.offer(makeTraverser("a", 1)), is(false));
        assertThat(ts.offer(makeTraverser("b", 1)), is(true));

        final Iterator<Traverser.Admin<String>> itty = ts.iterator();
        final Traverser.Admin<String> a = itty.next();
        assertEquals(7, a.bulk());
        final Traverser.Admin<String> b = itty.next();
        assertEquals(1, b.bulk());

        assertThat(itty.hasNext(), is(false));
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldRemoveTraverserAndThrowBecauseEmpty() {
        traverserSetMaker.get().remove();
    }

    @Test
    public void shouldRemoveTraverser() {
        final TraverserSet<String> ts = makeStringTraversers();
        assertEquals(4, ts.size());
        assertEquals(5, ts.bulkSize());
        ts.remove();
        assertEquals(3, ts.size());
        assertEquals(3, ts.bulkSize());
    }

    private TraverserSet<String> makeStringTraversers() {
        final TraverserSet<String> ts = traverserSetMaker.get();
        ts.add(makeTraverser("a", 1));
        ts.add(makeTraverser("a", 1));
        ts.add(makeTraverser("b1", 1));
        ts.add(makeTraverser("b2", 1));
        ts.add(makeTraverser("c", 1));
        return ts;
    }

    private <T> Traverser.Admin<T> makeTraverser(final T val, final long bulk) {
        return new B_O_Traverser<>(val, bulk).asAdmin();
    }
}
