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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the ChildTraversalVerificationStrategy (strategy-time layer).
 */
public class ChildTraversalVerificationStrategyTest {

    private final GraphTraversalSource g = EmptyGraph.instance().traversal();

    @Test
    public void shouldBeRegisteredByDefault() {
        // The strategy should be present in the default Graph strategy set (not EmptyGraph which has none)
        assertTrue(TraversalStrategies.GlobalCache
                .getStrategies(Graph.class)
                .getStrategy(ChildTraversalVerificationStrategy.class).isPresent());
    }

    @Test
    public void shouldBeRemovableViaWithoutStrategies() {
        // Users should be able to remove the strategy if needed
        final GraphTraversalSource gNoVerify = g.withoutStrategies(ChildTraversalVerificationStrategy.class);
        assertFalse(gNoVerify.getStrategies().getStrategy(ChildTraversalVerificationStrategy.class).isPresent());
    }

    @Test
    public void shouldNotRejectValidReadOnlyTraversal() {
        // Valid traversal should pass strategy application without error
        final Traversal.Admin<?, ?> traversal = g.V().has("name", __.V().values("name")).asAdmin();
        traversal.applyStrategies();
        // If we get here without exception, the test passes
    }

    @Test
    public void shouldNotRejectValidLookupTraversal() {
        // Valid V(traversal) should pass
        final Traversal.Admin<?, ?> traversal = g.V().V(__.out("knows").id()).asAdmin();
        traversal.applyStrategies();
    }
}
