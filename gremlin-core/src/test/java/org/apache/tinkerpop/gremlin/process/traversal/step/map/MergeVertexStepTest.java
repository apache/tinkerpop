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

import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.function.TraverserSetSupplier;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MergeVertexStepTest {

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
}
