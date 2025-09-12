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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class NoneStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() { return Collections.singletonList(__.none(P.gt(0))); }

    @Test
    public void testReturnTypes() {
        assertTrue(__.__(new int[]{}).none(P.gt(7)).hasNext());
        assertArrayEquals(new int[] {5, 6}, __.__(new int[] {5, 8, 10}, new int[] {5, 6}).none(P.gte(7)).next());
        assertArrayEquals(new long[] {5L, 6L}, __.__(new long[] {5L, 8L, 10L}, new long[] {5L, 6L}).none(P.gte(7)).next());
        assertArrayEquals(new Long[] {5L, 6L}, __.__(1).constant(new Long[] {5L, 6L}).none(P.gte(7)).next());
        assertArrayEquals(new double[] {5.1, 6.5}, __.__(new double[] {5.5, 8.0, 10.1}, new double[] {5.1, 6.5}).none(P.gte(7.0)).next(), 0.01);
    }

    @Test
    public void testNullParameter() {
        final Throwable thrown = assertThrows(IllegalArgumentException.class, () -> __.__(new int[]{1}).none(null).hasNext());
        assertEquals("Input predicate to none step can't be null.", thrown.getMessage());
    }

    @Test
    public void testSetTraverser() {
        final Set<Integer> numbers = new HashSet<>();
        numbers.add(5);
        numbers.add(6);

        assertTrue(__.__(numbers).none(P.gt(7)).hasNext());
    }

    @Test
    public void testListIteratorTraverser() {
        final List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(11);

        assertTrue(__.__(numbers.iterator()).none(P.lt(10)).hasNext());
    }

    @Test
    public void testCornerCases() {
        final List validOne = new ArrayList() {{ add(20); }};
        final List validTwo = new ArrayList() {{ add(21); add(25);}};
        final List validThree = new ArrayList() {{ add(51); add(57); add(71); }};
        final List validIncorrectType = new ArrayList() {{ add(100); add("25"); }};
        final List validEmpty = new ArrayList();
        final List containsNull = new ArrayList() {{ add(50); add(null); add(60); }};
        final List invalidIncorrectType = new ArrayList() {{ add(2); add("25"); }};
        final List valueTooSmall = new ArrayList() {{ add(101); add(1); add(10);}};

        assertEquals(6L, __.__(validOne, null, containsNull, validEmpty, invalidIncorrectType, validIncorrectType, valueTooSmall, validTwo, validThree)
                .none(P.lte(3)).count().next().longValue());
    }
}
