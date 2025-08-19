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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class HasStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.has("name"),
                __.has("name", "marko"),
                __.has("name", out("knows").values("name")),
                __.hasId(1),
                __.hasId(1.0),
                __.hasKey("name"),
                __.hasKey("age"),
                __.hasLabel("person"),
                __.hasLabel("project"),
                __.hasNot("name"),
                __.hasNot("age"),
                __.hasValue("marko"),
                __.hasValue("josh"),
                __.has("name", GValue.of("name", "marko")),
                __.has("name", out(GValue.of("label", "knows")).values("name")),
                __.hasId(GValue.of("idInt", 1)),
                __.hasId(GValue.of("idDouble", 1.0)),
                __.hasLabel(GValue.of("label", "person")),
                __.hasLabel(GValue.of("label", "project")),
                __.hasValue(GValue.of("name", "marko")),
                __.hasValue(GValue.of("name", "josh"))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.has("name", GValue.of("name", "marko")), Set.of("name")),
                Pair.of(__.has("name", out(GValue.of("label", "knows")).values("name")), Set.of("label")),
                Pair.of(__.hasId(GValue.of("id", 1)), Set.of("id")),
                Pair.of(__.hasId(GValue.of("id", 1.0)), Set.of("id")),
                Pair.of(__.hasLabel(GValue.of("label", "person")), Set.of("label")),
                Pair.of(__.hasLabel(GValue.of("label", "project")), Set.of("label")),
                Pair.of(__.hasValue(GValue.of("name", "marko")), Set.of("name")),
                Pair.of(__.hasValue(GValue.of("name", "josh")), Set.of("name"))
        );
    }

    /**
     * This test ensures that `has[Id|Label|Key|Value]` are compatible with the old varargs method signatures.
     */
    @Test
    public void testVarargsCompatibility() {
        final List<List<Traversal>> traversalLists = Arrays.asList(
                // hasId(Object id, Object... moreIds) should be compatible with hasId(Object... ids)
                Arrays.asList(
                        __.hasId(1),
                        __.hasId(eq(1)),
                        __.hasId(new Integer[]{1}),
                        __.hasId(Collections.singletonList(1))),
                Arrays.asList(
                        __.hasId(1, 2),
                        __.hasId(within(1, 2)),
                        __.hasId(new Integer[]{1, 2}),
                        __.hasId(Arrays.asList(1, 2)),
                        __.hasId(Collections.singletonList(1), Collections.singletonList(2))),

                // hasLabel(Object label, Object... moreLabels) should be compatible with hasLabel(Object... labels)
                Arrays.asList(
                        __.hasLabel("person"),
                        __.hasLabel(eq("person"))),
                Arrays.asList(
                        __.hasLabel("person", "software"),
                        __.hasLabel(within("person", "software"))),

                // hasKey(Object key, Object... moreKeys) should be compatible with hasKey(Object... keys)
                Arrays.asList(
                        __.hasKey("name"),
                        __.hasKey(eq("name"))),
                Arrays.asList(
                        __.hasKey("name", "age"),
                        __.hasKey(within("name", "age"))),

                // hasValue(Object value, Object... moreValues) should be compatible with hasValue(Object... values)
                Arrays.asList(
                        __.hasValue("marko"),
                        __.hasValue(eq("marko")),
                        __.hasValue(new String[]{"marko"})),
                Arrays.asList(
                        __.hasValue("marko", 32),
                        __.hasValue(within("marko", 32)),
                        __.hasValue(new Object[]{"marko", 32}))
        );
        
        for (final List<Traversal> traversals : traversalLists) {
            for (Traversal traversal1 : traversals) {
                final Step step1 = traversal1.asAdmin().getEndStep();
                assertEquals(step1, step1.clone());
                assertEquals(step1.hashCode(), step1.clone().hashCode());
                for (Traversal traversal2 : traversals) {
                    final Step step2 = traversal2.asAdmin().getEndStep();
                    assertEquals(step1, step2);
                    assertEquals(step1.hashCode(), step2.hashCode());
                }
            }
        }
    }

    @Test
    public void getPredicatesShouldPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getHasGValueTraversal();
        assertEquals(List.of("marko", 25), ((HasStep<?>) traversal.getSteps().get(0))
                .getPredicates().stream().map(P::getValue).collect(Collectors.toList()));
        verifyVariables(traversal, Set.of("n", "a"), Set.of());
    }

    @Test
    public void getPredicatesGValueSafeShouldNotPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getHasGValueTraversal();
        assertEquals(List.of("marko", 25), ((HasStep<?>) traversal.getSteps().get(0))
                .getPredicatesGValueSafe().stream().map(P::getValue).collect(Collectors.toList()));
        verifyVariables(traversal, Set.of(), Set.of("n", "a"));
    }

    @Test
    public void getPredicatesFromConcreteStep() {
        GraphTraversal.Admin<?, ?> traversal = getHasGValueTraversal();
        assertEquals(List.of("marko", 25), ((HasStep<?>) traversal.getSteps().get(0))
                .asConcreteStep().getPredicates().stream().map(P::getValue).collect(Collectors.toList()));
    }

    @Test
    public void getGValuesShouldReturnAllGValues() {
        GraphTraversal.Admin<?, ?> traversal = getHasGValueTraversal();
        Collection<GValue<?>> gValues = ((HasStep<?>) traversal.getSteps().get(0)).getGValues();
        assertEquals(2, gValues.size());
        assertTrue(gValues.stream().map(GValue::getName).collect(Collectors.toList())
                .containsAll(List.of("n", "a")));
    }

    @Test
    public void getGValuesNonShouldReturnEmptyCollection() {
        GraphTraversal.Admin<?, ?> traversal = __.has("name", "marko")
                .has("age", P.gt(25))
                .asAdmin();
        assertTrue(((HasContainerHolder<?,?>) traversal.getSteps().get(0)).getGValues().isEmpty());
    }

    private GraphTraversal.Admin<?, ?> getHasGValueTraversal() {
        return __.has("name", GValue.of("n", "marko"))
                .has("age", P.gt(GValue.of("a", 25)))
                .asAdmin();
    }
}
