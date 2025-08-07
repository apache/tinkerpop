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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.function.TraverserSetSupplier;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MergeEdgeStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.mergeE(Map.of()),
                __.mergeE(Map.of("name", "marko")),
                __.mergeE(Map.of("name", "marko")).option(Merge.onMatch, Map.of()),
                __.mergeE(Map.of("name", "marko")).option(Merge.onMatch, Map.of("age", 29)),
                __.mergeE(Map.of("name", "marko")).option(Merge.onCreate, Map.of()),
                __.mergeE(Map.of("name", "marko")).option(Merge.onCreate, Map.of("age", 29)),
                __.mergeE(Map.of("name", "marko")).option(Merge.onMatch, Map.of()).option(Merge.onCreate, Map.of()),
                __.mergeE(Map.of("name", "marko")).option(Merge.onMatch, Map.of("age", 29)).option(Merge.onCreate, Map.of("age", 30)),
                __.mergeE(GValue.of("mergeMap", Map.of("name", "marko"))),
                __.mergeE(GValue.of("mergeMap", Map.of("name", "marko"))).option(Merge.onMatch, GValue.of("matchMap", Map.of("age", 29))),
                __.mergeE(GValue.of("mergeMap", Map.of("name", "marko"))).option(Merge.onCreate, GValue.of("createMap", Map.of("age", 29))),
                __.mergeE(Map.of("name", "marko")).option(Merge.onMatch, GValue.of("matchMap", Map.of("age", 29))).option(Merge.onCreate, GValue.of("createMap", Map.of("age", 30)))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.mergeE(GValue.of("mergeMap", Map.of("name", "marko"))), Set.of("mergeMap")),
                Pair.of(__.mergeE(GValue.of("mergeMap", Map.of("name", "marko"))).option(Merge.onMatch, GValue.of("matchMap", Map.of("age", 29))), Set.of("mergeMap", "matchMap")),
                Pair.of(__.mergeE(GValue.of("mergeMap", Map.of("name", "marko"))).option(Merge.onCreate, GValue.of("createMap", Map.of("age", 29))), Set.of("mergeMap", "createMap")),
                Pair.of(__.mergeE(Map.of("name", "marko")).option(Merge.onMatch, GValue.of("matchMap", Map.of("age", 29))).option(Merge.onCreate, GValue.of("createMap", Map.of("age", 30))), Set.of("matchMap", "createMap"))
        );
    }

    @Test
    public void shouldValidateWithTokens() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                Direction.IN, 101,
                Direction.OUT, new ReferenceVertex(100),
                T.label, "knows",
                T.id, 10000);
        MergeEdgeStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithTokensBecauseOfBOTH() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                Direction.IN, 101,
                Direction.BOTH, new ReferenceVertex(100),
                T.label, "knows",
                T.id, 10000);
        MergeEdgeStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithTokensBecauseOfValue() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                Direction.IN, 101,
                Direction.OUT, new ReferenceVertex(100),
                T.value, "knows",
                T.id, 10000);
        MergeEdgeStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithTokensBecauseOfBadLabel() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                Direction.IN, 101,
                Direction.OUT, new ReferenceVertex(100),
                T.label, 100000,
                T.id, 10000);
        MergeEdgeStep.validateMapInput(m, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithTokensBecauseOfWeirdKey() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                Direction.IN, 101,
                Direction.OUT, new ReferenceVertex(100),
                new ReferenceVertex("weird"), 100000,
                T.id, 10000);
        MergeEdgeStep.validateMapInput(m, false);
    }

    @Test
    public void shouldValidateWithoutTokens() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v");
        MergeEdgeStep.validateMapInput(m, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithoutTokens() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                Direction.IN, 101,
                Direction.BOTH, new ReferenceVertex(100),
                T.label, "knows",
                T.id, 10000);
        MergeEdgeStep.validateMapInput(m, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToValidateWithoutTokensBecauseOfWeirdKey() {
        final Map<Object,Object> m = CollectionUtil.asMap("k", "v",
                new ReferenceVertex("weird"), 100000);
        MergeEdgeStep.validateMapInput(m, true);
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

        final MergeEdgeStep step = new MergeEdgeStep<>(traversal, true);
        step.addChildOption(Merge.onCreate, onCreateTraversal);

        final Map mergeMap = CollectionUtil.asMap("key2", "value2");
        final Map onCreateMap = step.onCreateMap(traverser, new LinkedHashMap<>(), mergeMap);

        assertEquals(CollectionUtil.asMap("key1", "value1", "key2", "value2"), onCreateMap);
    }
}
