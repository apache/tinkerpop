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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class IntersectStepTest extends StepTest {
    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.intersect(Collections.emptyList()),
                __.intersect(Collections.emptySet()),
                __.intersect(__.V().fold()));
    }

    @Test
    public void testReturnTypes() {
        assertEquals(Collections.emptySet(), __.__(Collections.emptyList()).intersect(Collections.emptyList()).next());
        assertEquals(Collections.emptySet(), __.__(Collections.emptyList()).intersect(Arrays.asList(1, 2, 3)).next());
        assertEquals(Collections.emptySet(), __.__(Arrays.asList(1, 2, 3)).intersect(Collections.emptyList()).next());

        assertEquals(new HashSet(Arrays.asList(8L, 10L)), __.__(new long[] {5L, 8L, 10L}).intersect(new long[] {7L, 8L, 10L}).next());
        assertEquals(Collections.emptySet(), __.__(new long[] {5L, 8L}).intersect(__.constant(new long[] {7L, 9L, 10L})).next());

        assertEquals(new HashSet(Arrays.asList(7L)), __.__(1).constant(new Long[] {7L}).intersect(new Long[] {7L, 8L, 11L}).next());
        assertEquals(new HashSet(Arrays.asList(7L)), __.__(1).constant(new Long[] {7L}).intersect(__.constant(new Long[] {7L, 8L, 11L})).next());

        assertEquals(new HashSet(Arrays.asList(8.0, 10.1)), __.__(new double[] {5.5, 8.0, 10.1}).intersect(new double[] {8.0, 10.1, 10.5}).next());
        assertEquals(new HashSet(Arrays.asList(8.0)), __.__(new double[] {5.5, 8.0, 10.1}).intersect(__.constant(new double[] {8.0, 10.5})).next());

        final Set<Integer> setA = new HashSet<>();
        setA.add(10); setA.add(11); setA.add(12);
        final Set<Integer> setB = new HashSet<>();
        setB.add(10); setB.add(11); setB.add(15);
        assertEquals(new HashSet(Arrays.asList(10, 11)), __.__(setA).intersect(setB).next());
        assertEquals(new HashSet(Arrays.asList(10, 11)), __.__(setA).intersect(__.constant(setB)).next());
    }

}
