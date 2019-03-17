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
package org.apache.tinkerpop.machine.traverser;

import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserSetTest {

    @Test
    public void shouldAddContainTraversers() {
        final TraverserSet<Long, String> traverserSet = new TraverserSet<>();
        Traverser<Long, String> a = new COPTraverser<>(LongCoefficient.create(10L), "hello");
        Traverser<Long, String> b = new COPTraverser<>(LongCoefficient.create(5L), "hello");
        Traverser<Long, String> c = new COPTraverser<>(LongCoefficient.create(3L), "world");
        ///
        assertTrue(traverserSet.isEmpty());
        assertEquals(0, traverserSet.size());
        assertEquals(0L, traverserSet.count());
        assertFalse(traverserSet.contains(a));
        assertFalse(traverserSet.contains(b));
        assertFalse(traverserSet.contains(c));
        //
        traverserSet.add(a);
        assertFalse(traverserSet.isEmpty());
        assertEquals(1, traverserSet.size());
        assertEquals(10L, traverserSet.count());
        assertTrue(traverserSet.contains(a));
        assertTrue(traverserSet.contains(b));
        assertFalse(traverserSet.contains(c));
        //
        traverserSet.add(b);
        assertFalse(traverserSet.isEmpty());
        assertEquals(1, traverserSet.size());
        assertEquals(15L, traverserSet.count());
        Traverser<Long, String> t = traverserSet.get(a);
        assertEquals("hello", t.object());
        assertEquals(15L, t.coefficient().value());
        t = traverserSet.get(b);
        assertEquals("hello", t.object());
        assertEquals(15L, t.coefficient().value());
        assertTrue(traverserSet.contains(a));
        assertTrue(traverserSet.contains(b));
        assertFalse(traverserSet.contains(c));
        //
        traverserSet.add(c);
        assertFalse(traverserSet.isEmpty());
        assertEquals(2, traverserSet.size());
        assertEquals(18L, traverserSet.count());
        t = traverserSet.get(b);
        assertEquals("hello", t.object());
        assertEquals(15L, t.coefficient().value());
        t = traverserSet.get(c);
        assertEquals("world", t.object());
        assertEquals(3L, t.coefficient().value());
        assertTrue(traverserSet.contains(a));
        assertTrue(traverserSet.contains(b));
        assertTrue(traverserSet.contains(c));
        //
        traverserSet.clear();
        assertTrue(traverserSet.isEmpty());
        assertEquals(0, traverserSet.size());
        assertEquals(0L, traverserSet.count());
        assertFalse(traverserSet.contains(a));
        assertFalse(traverserSet.contains(b));
        assertFalse(traverserSet.contains(c));
    }
}
