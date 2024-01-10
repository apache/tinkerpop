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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.junit.Test;

import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BulkSetTest {

    @Test
    public void shouldHaveProperHashAndEquality() {
        final BulkSet<String> a = new BulkSet<>();
        final BulkSet<String> b = new BulkSet<>();
        a.add("stephen", 12);
        a.add("marko", 32);
        a.add("daniel", 74);
        b.add("stephen", 12);
        b.add("marko", 32);
        b.add("daniel", 74);
        assertEquals(a, b);
        assertTrue(a.equals(b));
        assertEquals(a.hashCode(), b.hashCode());
        assertTrue(a.hashCode() == b.hashCode());
        assertEquals(12, a.get("stephen"));
        assertEquals(12, b.get("stephen"));
        a.add("matthias", 99);
        assertFalse(a.equals(b));
        assertFalse(a.hashCode() == b.hashCode());
        assertNotEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void shouldHaveProperCountAndNotOutOfMemoryException() {
        final Set<Boolean> list = new BulkSet<>();
        final Random random = new Random();
        for (int i = 0; i < 10000000; i++) {
            list.add(random.nextBoolean());
        }
        assertEquals(10000000, list.size());
    }

    @Test
    public void shouldHaveSameClassAsLongAsSameTypesAreAdded() {
        final BulkSet<Object> bulkSet = new BulkSet<>();
        bulkSet.add(new ReferenceVertex("1"), 1);
        bulkSet.add(new ReferenceVertex("2"), 1);
        bulkSet.add(new ReferenceVertex("3"), 3);
        assertTrue(bulkSet.allContainedElementsSameClass());
        assertEquals(bulkSet.getAllContainedElementsClass(), ReferenceVertex.class);
        bulkSet.add(new ReferenceVertex("4"), 5);
        assertEquals(bulkSet.getAllContainedElementsClass(), ReferenceVertex.class);
        bulkSet.add(new Long(2), 5);
        assertFalse(bulkSet.allContainedElementsSameClass());
        assertNull(bulkSet.getAllContainedElementsClass());
    }

    @Test
    public void shouldNotHaveSameClassForDifferentTypes() {
        final BulkSet<Object> bulkSet = new BulkSet<>();
        bulkSet.add(new ReferenceVertex("1"), 1);
        bulkSet.add(new Long(2), 5);
        bulkSet.add(new ReferenceVertex("3"), 3);
        assertNull(bulkSet.getAllContainedElementsClass());
        assertFalse(bulkSet.allContainedElementsSameClass());
        bulkSet.add(new ReferenceVertex("4"), 5);
        assertNull(bulkSet.getAllContainedElementsClass());
        assertFalse(bulkSet.allContainedElementsSameClass());
    }

    @Test
    public void shouldNotHaveSameClassWhenEmpty() {
        final BulkSet<Object> bulkSet = new BulkSet<>();
        assertNull(bulkSet.getAllContainedElementsClass());
        assertFalse(bulkSet.allContainedElementsSameClass());
    }

    @Test
    public void shouldNotHaveSameClassForNull() {
        final BulkSet<Object> bulkSet = new BulkSet<>();
        bulkSet.add(null, 5);
        assertNull(bulkSet.getAllContainedElementsClass());
        assertFalse(bulkSet.allContainedElementsSameClass());
        assertFalse(bulkSet.isEmpty());
        bulkSet.add(new Long(2), 5);
        assertNull(bulkSet.getAllContainedElementsClass());
        assertFalse(bulkSet.allContainedElementsSameClass());
        bulkSet.add(null, 3);
        assertNull(bulkSet.getAllContainedElementsClass());
        assertFalse(bulkSet.allContainedElementsSameClass());

        // adding null in the middle
        final BulkSet<Object> bulkSet2 = new BulkSet<>();
        bulkSet2.add(new ReferenceVertex("4"), 5);
        assertEquals(bulkSet2.getAllContainedElementsClass(), ReferenceVertex.class);
        assertTrue(bulkSet2.allContainedElementsSameClass());
        assertFalse(bulkSet2.isEmpty());
        bulkSet2.add(null, 5);
        assertNull(bulkSet2.getAllContainedElementsClass());
        assertFalse(bulkSet2.allContainedElementsSameClass());
        bulkSet2.add(new ReferenceVertex("3"), 5);
        assertNull(bulkSet2.getAllContainedElementsClass());
        assertFalse(bulkSet2.allContainedElementsSameClass());
    }

    @Test
    public void shouldHaveCorrectBulkCounts() {
        final BulkSet<String> set = new BulkSet<>();
        set.add("marko");
        set.add("matthias");
        set.add("marko", 7);
        set.add("stephen");
        set.add("stephen");
        assertEquals(8, set.get("marko"));
        assertEquals(1, set.get("matthias"));
        assertEquals(2, set.get("stephen"));
        final Iterator<String> iterator = set.iterator();
        for (int i = 0; i < 11; i++) {
            if (i < 8)
                assertEquals("marko", iterator.next());
            else if (i < 9)
                assertEquals("matthias", iterator.next());
            else
                assertEquals("stephen", iterator.next());
        }
        assertEquals(11, set.size());
    }
}
