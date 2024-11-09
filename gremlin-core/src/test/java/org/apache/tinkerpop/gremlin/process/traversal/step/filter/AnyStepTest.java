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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AnyStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() { return Collections.singletonList(__.any(P.gt(0))); }

    @Test
    public void testReturnTypes() {
        assertFalse(__.__(new int[] {}).any(P.gt(7)).hasNext());
        assertArrayEquals(new int[] {7, 10}, __.__(new int[] {3, 5, 6}, new int[] {7, 10}).any(P.gte(7)).next());
        assertArrayEquals(new long[] {7L, 10L}, __.__(new long[] {3L, 5L, 6L}, new long[] {7L, 10L}).any(P.gte(7)).next());
        assertArrayEquals(new Long[] {7L, 10L}, __.__(1).constant(new Long[] {7L, 10L}).any(P.gte(7)).next());
        assertArrayEquals(new double[] {7.7, 10.1}, __.__(new double[] {3.3, 5.791739562, 6.1082}, new double[] {7.7, 10.1}).any(P.gte(7)).next(), 0.01);
        assertArrayEquals(new Object[] {2, "hello", 10L}, __.__(new Object[] {3.6, "other"}, new Object[] {2, "hello", 10L}).any(P.gte(7)).next());
    }

    @Test
    public void testNullParameter() {
        final Throwable thrown = assertThrows(IllegalArgumentException.class, () -> __.__(new int[]{1}).any(null).hasNext());
        assertEquals("Input predicate to any step can't be null.", thrown.getMessage());
    }

    @Test
    public void testSetTraverser() {
        final Set<Integer> numbers = new HashSet<>();
        numbers.add(10);
        numbers.add(11);

        assertTrue(__.__(numbers).any(P.eq(10)).hasNext());
    }

    @Test
    public void testListIteratorTraverser() {
        final List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(11);

        assertTrue(__.__(numbers.iterator()).any(P.eq(11)).hasNext());
    }

    @Test
    public void testCornerCases() {
        final List validOne = new ArrayList() {{ add(20); }};
        final List validTwo = new ArrayList() {{ add(21); add(25);}};
        final List validThree = new ArrayList() {{ add(51); add(57); add(71); }};
        final List validFour = new ArrayList() {{ add(1); add(2); add(10); }};
        final List invalidOne = new ArrayList() {{ add(0); add(1); }};
        final List containsNull = new ArrayList() {{ add(50); add(null); add(60); }};
        final List empty = new ArrayList();
        final List incorrectType = new ArrayList() {{ add(100); add("25"); }};
        final List valueTooSmall = new ArrayList() {{ add(101); add(1); add(10);}};

        assertEquals(7L, __.__(validOne, null, containsNull, empty, incorrectType, validFour, invalidOne, valueTooSmall, validTwo, validThree)
                        .any(P.gt(3)).count().next().longValue());
    }
}
