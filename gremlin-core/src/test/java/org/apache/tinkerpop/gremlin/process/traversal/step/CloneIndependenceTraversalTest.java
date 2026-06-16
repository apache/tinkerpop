/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsSame.sameInstance;

/**
 * Property 8: Clone independence for traversal-bearing steps.
 * <p>
 * For any step containing child traversal arguments, cloning the step SHALL produce a deep copy
 * where modifying the clone's child traversal state does NOT affect the original step's child
 * traversal state.
 */
public class CloneIndependenceTraversalTest {

    @Test
    public void shouldProduceIndependentCloneForHasStepWithTraversalValue() {
        // Create a HasStep with a traversal-bearing HasContainer (P.eq(traversal))
        final Traversal.Admin<?, ?> childTraversal = __.constant("marko").asAdmin();
        final HasContainer hc = new HasContainer("name", P.eq(childTraversal));
        final HasStep<Vertex> original = new HasStep<>(EmptyTraversal.instance(), hc);

        // Clone the step
        final HasStep<Vertex> clone = original.clone();

        // Verify the clone has child traversals
        final List<Traversal.Admin<?, ?>> originalChildren = (List) original.getLocalChildren();
        final List<Traversal.Admin<?, ?>> cloneChildren = (List) clone.getLocalChildren();

        assertThat(originalChildren.size(), is(1));
        assertThat(cloneChildren.size(), is(1));

        // The child traversals should be different instances (deep copy)
        assertThat(cloneChildren.get(0), is(not(sameInstance(originalChildren.get(0)))));

        // Verify HasContainers are also independent
        final List<HasContainer> originalContainers = original.getHasContainers();
        final List<HasContainer> cloneContainers = clone.getHasContainers();
        assertThat(cloneContainers.get(0), is(not(sameInstance(originalContainers.get(0)))));
        assertThat(cloneContainers.get(0).getPredicate().getChildTraversals().get(0),
                is(not(sameInstance(originalContainers.get(0).getPredicate().getChildTraversals().get(0)))));
    }

    @Test
    public void shouldProduceIndependentCloneForGraphStepWithIdTraversal() {
        // Create a GraphStep with an idTraversal
        final Traversal.Admin<?, ?> idTraversal = __.select("ids").asAdmin();
        final GraphStep<Vertex, Vertex> original =
                new GraphStep<>(EmptyTraversal.instance(), Vertex.class, false, idTraversal);

        // Clone the step
        final GraphStep<Vertex, Vertex> clone = original.clone();

        // Verify the clone has a child traversal
        assertThat(original.getIdTraversal(), is(notNullValue()));
        assertThat(clone.getIdTraversal(), is(notNullValue()));

        // The idTraversal should be a different instance (deep copy)
        assertThat(clone.getIdTraversal(), is(not(sameInstance(original.getIdTraversal()))));
    }

    @Test
    public void shouldProduceIndependentCloneForHasStepWithMultipleContainers() {
        // Create a HasStep with multiple HasContainers, some with traversals
        final HasContainer hc1 = new HasContainer("name", P.eq(__.constant("marko").asAdmin()));
        final HasContainer hc2 = new HasContainer("age", P.eq(__.constant(29).asAdmin()));
        final HasStep<Vertex> original = new HasStep<>(EmptyTraversal.instance(), hc1, hc2);

        // Clone the step
        final HasStep<Vertex> clone = original.clone();

        // Verify all containers are independent
        final List<HasContainer> originalContainers = original.getHasContainers();
        final List<HasContainer> cloneContainers = clone.getHasContainers();

        assertThat(originalContainers.size(), is(2));
        assertThat(cloneContainers.size(), is(2));

        for (int i = 0; i < originalContainers.size(); i++) {
            assertThat(cloneContainers.get(i), is(not(sameInstance(originalContainers.get(i)))));
            assertThat(cloneContainers.get(i).getPredicate().getChildTraversals().get(0),
                    is(not(sameInstance(originalContainers.get(i).getPredicate().getChildTraversals().get(0)))));
        }
    }

}
