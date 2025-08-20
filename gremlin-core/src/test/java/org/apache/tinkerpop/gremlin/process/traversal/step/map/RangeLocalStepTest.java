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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.step.map.RangeLocalStep.applyRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeStepPlaceholder;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class RangeLocalStepTest extends GValueStepTest {

    private static final String LOW_NAME = "low";
    private static final String HIGH_NAME = "high";
    private static final long LOW_VALUE = 1L;
    private static final long HIGH_VALUE = 10L;

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.limit(Scope.local, HIGH_VALUE),
                __.skip(Scope.local, 9L), // TODO:: best to edit this to __.skip(Scope.local, 10L) following resolution of TINKERPOP-3170
                __.range(Scope.local, LOW_VALUE, HIGH_VALUE),
                __.limit(Scope.local, GValue.of("limit", HIGH_VALUE)),
                __.skip(Scope.local, GValue.of("skip", 9L)),
                __.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE))
        );
    }

    @Test
    public void applyRangeShouldReturnCollectionForSingleElementFromList() {
        // Test that range(local) returns a collection even for single element results
        assertEquals(List.of(2), applyRange(List.of(1, 2, 3), 1, 2));
    }

    @Test
    public void applyRangeShouldReturnCollectionForSingleElementFromSet() {
        // Test that range(local) preserves Set type and returns a collection
        assertEquals(Set.of(2), applyRange(new LinkedHashSet<>(List.of(1, 2, 3)), 1, 2));
    }

    @Test
    public void applyRangeShouldReturnMultipleElementsFromList() {
        // Test that range(local) returns multiple elements as expected
        assertEquals(List.of(2, 3, 4), applyRange(List.of(1, 2, 3, 4, 5), 1, 4));
    }

    @Test
    public void applyRangeShouldReturnEmptyCollectionForEmptyRange() {
        // Test that range(local) returns empty collection when range produces no results
        assertTrue(applyRange(List.of(1, 2, 3), 5, 10).isEmpty());
    }

    @Test
    public void applyRangeShouldReturnSingletonMap() {
        // Test that Map behavior is unchanged - should return Map with selected entries
        final Map<String, Integer> input = new LinkedHashMap<>();
        input.put("a", 1);
        input.put("b", 2);
        input.put("c", 3);

        final Map<String, Integer> result = applyRange(input, 1, 2);
        assertEquals(1, result.size());
        assertEquals((Integer) 2, result.get("b"));
    }

    @Test
    public void applyRangeShouldReturnMapWithMultipleEntries() {
        // Test that Map behavior works correctly with multiple entries
        final Map<String, Integer> input = new LinkedHashMap<>();
        input.put("a", 1);
        input.put("b", 2);
        input.put("c", 3);
        input.put("d", 4);

        final Map<String, Integer> result = applyRange(input, 1, 3);
        assertEquals(2, result.size());
        assertEquals((Integer) 2, result.get("b"));
        assertEquals((Integer) 3, result.get("c"));
    }

    @Test
    public void applyRangeShouldHandleNonCollectionObjects() {
        // Test that non-collection objects are returned as-is
        final String input = "test";
        final Object result = applyRange(input, 0, 1);
        assertEquals(input, result);
    }

    @Test
    public void applyRangeShouldHandleNullInput() {
        // Test that null input is returned as-is
        assertNull(applyRange(null, 0, 1));
    }

    @Test
    public void applyRangeShouldHandleArrayInput() {
        // Test that array input is converted to collection and processed
        final Object result = applyRange(new int[]{1, 2, 3, 4, 5}, 1, 3);
        assertEquals(List.of(2, 3), result);
    }

    @Test
    public void applyRangeShouldHandleLimitLocalEquivalent() {
        // Test that limit(local, n) equivalent to range(local, 0, n) returns collection
        assertEquals(List.of(1), applyRange(List.of(1, 2, 3, 4, 5), 0, 1));
    }

    @Test
    public void applyRangeShouldHandleTailLocalEquivalent() {
        // Test that tail(local, n) equivalent behavior returns collection
        assertEquals(List.of(5), applyRange(List.of(1, 2, 3, 4, 5), 4, 5));
    }

    @Test
    public void applyRangeShouldHandleUnboundedRange() {
        // Test that unbounded range (high = -1) works correctly
        assertEquals(List.of(3, 4, 5), applyRange(List.of(1, 2, 3, 4, 5), 2, -1));
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.limit(Scope.local, GValue.of("limit", HIGH_VALUE)), Set.of("limit")),
                Pair.of(__.skip(Scope.local, GValue.of("skip", HIGH_VALUE)), Set.of("skip")),
                Pair.of(__.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)), Set.of(LOW_NAME, HIGH_NAME))
        );
    }

    @Test
    public void getLowHighRangeNonGValue() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(Scope.local, LOW_VALUE, HIGH_VALUE).asAdmin();
        assertEquals((Long) LOW_VALUE, ((RangeLocalStep) traversal.getSteps().get(0)).getLowRange());
        assertEquals((Long) HIGH_VALUE, ((RangeLocalStep) traversal.getSteps().get(0)).getHighRange());
        verifyNoVariables(traversal);
    }

    @Test
    public void getLowHighRangeGValueSafeShouldNotPinVariables() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals(LOW_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).getLowRangeGValueSafe());
        assertEquals(HIGH_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).getHighRangeGValueSafe());
        verifyVariables(traversal, Set.of(), Set.of(LOW_NAME, HIGH_NAME));
    }

    @Test
    public void getLowShouldPinVariable() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals((Long) LOW_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).getLowRange());
        verifyVariables(traversal, Set.of(LOW_NAME), Set.of(HIGH_NAME));
    }

    @Test
    public void getHighShouldPinVariable() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals((Long) HIGH_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).getHighRange());
        verifyVariables(traversal, Set.of(HIGH_NAME), Set.of(LOW_NAME));
    }

    @Test
    public void getLowHighRangeGValueFromConcreteStep() {
        GraphTraversal.Admin<Object, Object> traversal = __.range(Scope.local, GValue.of(LOW_NAME, LOW_VALUE), GValue.of(HIGH_NAME, HIGH_VALUE)).asAdmin();
        assertEquals((Long) LOW_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getLowRange());
        assertEquals((Long) HIGH_VALUE, ((RangeStepPlaceholder.RangeLocalStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getHighRange());
    }
}
