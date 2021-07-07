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

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MultiIteratorTest {
    @Test
    public void shouldNotHaveNextIfNoIteratorsAreAdded() {
        final Iterator<String> itty = new MultiIterator<>();
        assertThat(itty.hasNext(), is(false));
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowFastNoSuchElementExceptionIfNoIteratorsAreAdded() {
        final Iterator<String> itty = new MultiIterator<>();
        itty.next();
    }

    @Test
    public void shouldNotHaveNextIfEmptyIteratorIsAdded() {
        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(EmptyIterator.instance());
        assertThat(itty.hasNext(), is(false));
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowFastNoSuchElementExceptionIfEmptyIteratorIsAdded() {
        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(EmptyIterator.instance());
        itty.next();
    }

    @Test
    public void shouldNotHaveNextIfEmptyIteratorsAreAdded() {
        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        assertThat(itty.hasNext(), is(false));
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowFastNoSuchElementExceptionIfEmptyIteratorsAreAdded() {
        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        itty.next();
    }

    @Test
    public void shouldIterateWhenMultipleIteratorsAreAdded() {
        final List<String> list = new ArrayList<>();
        list.add("test1");
        list.add("test2");
        list.add("test3");

        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(list.iterator());

        assertThat(itty.hasNext(), is(true));
        assertEquals("test1", itty.next());
        assertEquals("test2", itty.next());
        assertEquals("test3", itty.next());
        assertThat(itty.hasNext(), is(false));
    }

    @Test
    public void shouldClearIterators() {
        final List<String> list = new ArrayList<>();
        list.add("test1");
        list.add("test2");
        list.add("test3");

        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(list.iterator());

        itty.clear();

        assertThat(itty.hasNext(), is(false));
    }

    @Test
    public void shouldCloseIterators() {

        final MultiIterator<String> itty = new MultiIterator<>();
        final DummyAutoCloseableIterator<String> inner1 = new DummyAutoCloseableIterator<>();
        final DummyAutoCloseableIterator<String> inner2 = new DummyAutoCloseableIterator<>();
        itty.addIterator(inner1);
        itty.addIterator(inner2);

        itty.close();

        assertTrue(inner1.isClosed());
        assertTrue(inner2.isClosed());
    }

    // Dummy iterator to verify that its close method is called in the test.
    private static class DummyAutoCloseableIterator<T> implements Iterator<T>, AutoCloseable {
        private boolean closed;

        public DummyAutoCloseableIterator() {
            closed = false;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            return null;
        }

        @Override
        public void close() throws Exception {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
