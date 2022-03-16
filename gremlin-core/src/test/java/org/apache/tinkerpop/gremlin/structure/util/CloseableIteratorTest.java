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
package org.apache.tinkerpop.gremlin.structure.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CloseableIteratorTest {
    @Test
    public void shouldWrapIterator()  {
        final List<String> stuff = Arrays.asList("this stuff", "that stuff", "other stuff");
        final CloseableIterator<String> itty = new DefaultCloseableIterator<>(stuff.iterator());
        assertThat(itty.hasNext(), is(true));
        assertEquals("this stuff", itty.next());
        assertThat(itty.hasNext(), is(true));
        assertEquals("that stuff", itty.next());
        assertThat(itty.hasNext(), is(true));
        assertEquals("other stuff", itty.next());
        assertThat(itty.hasNext(), is(false));

        // this is a do-nothing, but we should still be able to call it
        itty.close();
    }

    @Test
    public void shouldWrapIteratorWithHelper() {
        final List<String> stuff = Arrays.asList("this stuff", "that stuff", "other stuff");
        final CloseableIterator<String> itty = CloseableIterator.of(stuff.iterator());
        assertThat(itty.hasNext(), is(true));
        assertEquals("this stuff", itty.next());
        assertThat(itty.hasNext(), is(true));
        assertEquals("that stuff", itty.next());
        assertThat(itty.hasNext(), is(true));
        assertEquals("other stuff", itty.next());
        assertThat(itty.hasNext(), is(false));

        // this is a do-nothing, but we should still be able to call it
        itty.close();
    }

    @Test
    public void shouldReturnSameInstance() {
        final List<String> stuff = Arrays.asList("this stuff", "that stuff", "other stuff");
        final CloseableIterator<String> itty = new DefaultCloseableIterator<>(stuff.iterator());
        final CloseableIterator<String> same = CloseableIterator.of(itty);
        assertSame(itty, same);
        assertThat(itty.hasNext(), is(true));
        assertEquals("this stuff", itty.next());
        assertThat(itty.hasNext(), is(true));
        assertEquals("that stuff", itty.next());
        assertThat(itty.hasNext(), is(true));
        assertEquals("other stuff", itty.next());
        assertThat(itty.hasNext(), is(false));
        assertThat(same.hasNext(), is(false));

        // this is a do-nothing, but we should still be able to call it
        itty.close();
    }
}
