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

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ArrayIteratorTest {
    @Test
    public void shouldIterateAnArray() {
        final String[] arr = new String[3];
        arr[0] = "test1";
        arr[1] = "test2";
        arr[2] = "test3";

        final Iterator<String> itty = new ArrayIterator<>(arr);
        assertEquals("test1", itty.next());
        assertEquals("test2", itty.next());
        assertEquals("test3", itty.next());

        assertFalse(itty.hasNext());
    }

    @Test
    public void shouldIterateAnArrayWithNull() {
        final String[] arr = new String[3];
        arr[0] = "test1";
        arr[1] = "test2";
        arr[2] = null;

        final Iterator<String> itty = new ArrayIterator<>(arr);
        assertEquals("test1", itty.next());
        assertEquals("test2", itty.next());
        assertNull(itty.next());

        assertFalse(itty.hasNext());
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowFastNoSuchElementException() {
        final Iterator<String> itty = new ArrayIterator<>(new String[0]);
        assertFalse(itty.hasNext());
        itty.next();
    }

    @Test
    public void shouldShowElementsInToString() {
        final String[] arr = new String[3];
        arr[0] = "test1";
        arr[1] = "test2";
        arr[2] = "test3";

        final Iterator<String> itty = new ArrayIterator<>(arr);
        assertEquals("[test1, test2, test3]", itty.toString());
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullConstruction() {
        new ArrayIterator<>(null);
    }

    @Test
    public void shouldAllowNullInArray() {
        final String[] arr = new String[3];
        arr[0] = "test1";
        arr[1] = null;
        arr[2] = "test3";

        final Iterator<String> itty = new ArrayIterator<>(arr);
        assertEquals("[test1, null, test3]", itty.toString());
    }

    @Test
    public void shouldAllowSingleNullInArray() {
        final String[] arr = new String[1];

        final Iterator<String> itty = new ArrayIterator<>(arr);
        assertEquals("[null]", itty.toString());
    }

    @Test
    public void shouldAllowZeroNullInArray() {
        final String[] arr = new String[0];

        final Iterator<String> itty = new ArrayIterator<>(arr);
        assertEquals("[]", itty.toString());
    }
}
