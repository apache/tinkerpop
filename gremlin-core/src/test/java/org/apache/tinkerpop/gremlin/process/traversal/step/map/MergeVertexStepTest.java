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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.function.TraverserSetSupplier;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MergeVertexStepTest extends GValueStepTest {

    private final Map<Object,Object> NAME_MAP = Map.of("name", "marko");
    private final Map<Object,Object> AGE_29_MAP = Map.of("age", 29);
    private final Map<Object,Object> AGE_30_MAP = Map.of("age", 30);

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.mergeV(Map.of()),
                __.mergeV(NAME_MAP),
                __.mergeV(NAME_MAP).option(Merge.onMatch, Map.of()),
                __.mergeV(NAME_MAP).option(Merge.onMatch, AGE_29_MAP),
                __.mergeV(NAME_MAP).option(Merge.onCreate, Map.of()),
                __.mergeV(NAME_MAP).option(Merge.onCreate, AGE_29_MAP),
                __.mergeV(NAME_MAP).option(Merge.onMatch, Map.of()).option(Merge.onCreate, Map.of()),
                __.mergeV(NAME_MAP).option(Merge.onMatch, AGE_29_MAP).option(Merge.onCreate, AGE_30_MAP),
                __.mergeV(GValue.of("mergeMap", NAME_MAP)),
                __.mergeV(GValue.of("mergeMap", NAME_MAP)).option(Merge.onMatch, GValue.of("matchMap", AGE_29_MAP)),
                __.mergeV(GValue.of("mergeMap", NAME_MAP)).option(Merge.onCreate, GValue.of("createMap", AGE_29_MAP)),
                __.mergeV(NAME_MAP).option(Merge.onMatch, GValue.of("matchMap", AGE_29_MAP)).option(Merge.onCreate, GValue.of("createMap", AGE_30_MAP))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.mergeV(GValue.of("mergeMap", NAME_MAP)), Set.of("mergeMap")),
                Pair.of(__.mergeV(GValue.of("mergeMap", NAME_MAP)).option(Merge.onMatch, GValue.of("matchMap", AGE_29_MAP)), Set.of("mergeMap", "matchMap")),
                Pair.of(__.mergeV(GValue.of("mergeMap", NAME_MAP)).option(Merge.onCreate, GValue.of("createMap", AGE_29_MAP)), Set.of("mergeMap", "createMap")),
                Pair.of(__.mergeV(NAME_MAP).option(Merge.onMatch, GValue.of("matchMap", AGE_29_MAP)).option(Merge.onCreate, GValue.of("createMap", Map.of("age", 30))), Set.of("matchMap", "createMap"))
        );
    }

    @Test
    public void shouldRemoveExistingPropertyFromMergeStep() {
        final MergeVertexStep<Object> step = new MergeVertexStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(),
                true);
        step.addProperty("name", "marko");
        step.addProperty("age", 29);
        assertTrue(step.getProperties().containsKey("name"));
        assertTrue(step.getProperties().containsKey("age"));

        assertTrue(step.removeProperty("name"));
        assertFalse(step.getProperties().containsKey("name"));
        assertTrue(step.getProperties().containsKey("age"));
        assertFalse(step.removeProperty("name"));
    }

    @Test
    public void shouldRemoveExistingPropertyFromMergeVertexStepPlaceholder() {
        final MergeVertexStepPlaceholder<Object> step = new MergeVertexStepPlaceholder<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(),
                true);
        step.addProperty("name", GValue.of("vadas"));
        step.addProperty("age", GValue.of(27));
        assertTrue(step.getPropertiesGValueSafe().containsKey("name"));
        assertTrue(step.getPropertiesGValueSafe().containsKey("age"));

        assertTrue(step.removeProperty("name"));
        assertFalse(step.getPropertiesGValueSafe().containsKey("name"));
        assertTrue(step.getPropertiesGValueSafe().containsKey("age"));
        assertFalse(step.removeProperty("name"));
    }

    @Test
    public void shouldValidateWithTokens() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                T.label, "person",
                T.id, 10000);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithTokensBecauseOfValue() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                T.value, "nope",
                T.label, "person",
                T.id, 10000);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithTokensBecauseOfWeirdKey() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                Direction.IN, "weird",
                T.id, 10000);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test
    public void shouldValidateWithoutTokens() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v");
        MergeVertexStep.validateMapInput(m, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithoutTokens() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                Direction.IN, 101,
                Direction.BOTH, new ReferenceVertex(100),
                T.label, "knows",
                T.id, 10000);
        MergeVertexStep.validateMapInput(m, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithoutTokensBecauseOfWeirdKey() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                new ReferenceVertex("weird"), 100000);
        MergeVertexStep.validateMapInput(m, true);
    }

    @Test
    public void shouldWorkWithImmutableMap() {
        final Traversal.Admin traversal = mock(Traversal.Admin.class);
        when(traversal.getTraverserSetSupplier()).thenReturn(TraverserSetSupplier.instance());
        when(traversal.getParent()).thenReturn(EmptyStep.instance());
        when(traversal.getStrategies()).thenReturn(new DefaultTraversalStrategies());
        when(traversal.getGValueManager()).thenReturn(new GValueManager());
        final Traverser.Admin traverser = mock(Traverser.Admin.class);
        when(traverser.split()).thenReturn(mock(Traverser.Admin.class));
        final Traversal.Admin onCreateTraversal = mock(Traversal.Admin.class);
        when(onCreateTraversal.next()).thenReturn(Collections.unmodifiableMap(CollectionUtil.asMap("key1", "value1")));
        when(onCreateTraversal.getSideEffects()).thenReturn(mock(TraversalSideEffects.class));
        when(onCreateTraversal.getGValueManager()).thenReturn(new GValueManager());

        final MergeVertexStep step = new MergeVertexStep(traversal, true);
        step.addChildOption(Merge.onCreate, onCreateTraversal);

        final Map mergeMap = CollectionUtil.asMap("key2", "value2");
        final Map onCreateMap = step.onCreateMap(traverser, mergeMap);

        assertEquals(CollectionUtil.asMap("key1", "value1", "key2", "value2"), onCreateMap);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithNullKey() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                null, "person");
        MergeVertexStep.validateMapInput(m, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithNullLabelValue() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                T.label, null);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithNullIdValue() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                T.id, null);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithNullMergeValue() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                Merge.inV, null);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithObjectAsLabelValue() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                T.label, new Object());
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithNullDirectionValue() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                Direction.IN, null);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithHiddenIdKey() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                "~id", 10000);
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithHiddenLabelKey() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                "~label", "person");
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithHiddenLabelValue() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                T.label, "~person");
        MergeVertexStep.validateMapInput(m, false);
    }

    @Test
    public void getMergeTraversalShouldPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        assertEquals(NAME_MAP, ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).getMergeTraversal().next());
        verifyVariables(traversal, Set.of("mergeMap"), Set.of("matchMap", "createMap"));
    }

    @Test
    public void getMergeTraversalGValueSafeShouldNotPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        assertEquals(NAME_MAP, ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).getMergeTraversalGValueSafe().next());
        verifyVariables(traversal, Set.of(), Set.of("mergeMap", "matchMap", "createMap"));
    }

    @Test
    public void getMergeTraversalFromConcreteStep() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        assertEquals(NAME_MAP, ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).asConcreteStep().getMergeTraversal().next());
    }

    @Test
    public void getOnCreateTraversalShouldPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        assertEquals(AGE_30_MAP, ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).getOnCreateTraversal().next());
        verifyVariables(traversal, Set.of("createMap"), Set.of("mergeMap", "matchMap"));
    }

    @Test
    public void getOnCreateTraversalGValueSafeShouldNotPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        assertEquals(AGE_30_MAP, ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).getOnCreateTraversalGValueSafe().next());
        verifyVariables(traversal, Set.of(), Set.of("mergeMap", "matchMap", "createMap"));
    }

    @Test
    public void getOnCreateTraversalFromConcreteStep() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        assertEquals(AGE_30_MAP, ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).asConcreteStep().getOnCreateTraversal().next());
    }

    @Test
    public void getOnMatchTraversalShouldPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        assertEquals(AGE_29_MAP, ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).getOnMatchTraversal().next());
        verifyVariables(traversal, Set.of("matchMap"), Set.of("mergeMap", "createMap"));
    }

    @Test
    public void getOnMatchTraversalGValueSafeShouldNotPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        assertEquals(AGE_29_MAP, ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).getOnMatchTraversalGValueSafe().next());
        verifyVariables(traversal, Set.of(), Set.of("mergeMap", "matchMap", "createMap"));
    }

    @Test
    public void getOnMatchTraversalFromConcreteStep() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        assertEquals(AGE_29_MAP, ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).asConcreteStep().getOnMatchTraversal().next());
    }

    @Test
    public void getPropertiesShouldPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        MergeVertexStepPlaceholder step = (MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0);
        //There is no direct way to add properties to mergeV via Gremlin, this interface is only exposed for the purposes of PartitionStrategy
        step.addProperty("key", GValue.of("x", "value"));
        assertEquals(List.of("value"), ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0))
                .getProperties().get("key"));
        verifyVariables(traversal, Set.of("x"), Set.of("mergeMap", "matchMap", "createMap"));
    }

    @Test
    public void getPropertiesGValueSafeShouldNotPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        MergeVertexStepPlaceholder step = (MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0);
        //There is no direct way to add properties to mergeV via Gremlin, this interface is only exposed for the purposes of PartitionStrategy
        step.addProperty("key", GValue.of("x", "value"));
        assertEquals(List.of("value"), ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0))
                .getPropertiesGValueSafe().get("key"));
        verifyVariables(traversal, Set.of(), Set.of("mergeMap", "matchMap", "createMap", "x"));
    }

    @Test
    public void getPropertiesFromConcreteStep() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        MergeVertexStepPlaceholder step = (MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0);
        //There is no direct way to add properties to mergeV via Gremlin, this interface is only exposed for the purposes of PartitionStrategy
        step.addProperty("key", GValue.of("x", "value"));
        assertEquals(List.of("value"), ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0))
                .asConcreteStep().getProperties().get("key"));
    }

    @Test
    public void getGValuesShouldReturnAllGValues() {
        GraphTraversal.Admin<?, ?> traversal = getMergeVGValueTraversal();
        Collection<GValue<?>> gValues = ((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).getGValues();
        assertEquals(3, gValues.size());
        assertTrue(gValues.stream().map(GValue::getName).collect(Collectors.toList())
                .containsAll(List.of("mergeMap", "matchMap", "createMap")));
    }

    @Test
    public void getGValuesNonShouldReturnEmptyCollection() {
        GraphTraversal.Admin<?, ?> traversal = __.mergeV(NAME_MAP)
                .option(Merge.onMatch,AGE_29_MAP)
                .option(Merge.onCreate, AGE_30_MAP)
                .asAdmin();
        assertTrue(((MergeVertexStepPlaceholder<?>) traversal.getSteps().get(0)).getGValues().isEmpty());
    }

    private GraphTraversal.Admin<?, ?> getMergeVGValueTraversal() {
        return __.mergeV(GValue.of("mergeMap", NAME_MAP))
                .option(Merge.onMatch, GValue.of("matchMap", AGE_29_MAP))
                .option(Merge.onCreate, GValue.of("createMap", AGE_30_MAP)).asAdmin();
    }
}
