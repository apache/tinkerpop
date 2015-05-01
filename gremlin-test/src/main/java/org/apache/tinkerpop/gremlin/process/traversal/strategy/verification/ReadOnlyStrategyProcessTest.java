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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@UseEngine(TraversalEngine.Type.STANDARD)
@UseEngine(TraversalEngine.Type.COMPUTER)
public class ReadOnlyStrategyProcessTest extends AbstractGremlinProcessTest {
    @Test
    public void shouldTraverseV() {
        assertTraversal(create().V(), false);
    }

    @Test
    public void shouldTraverseV_out() {
        assertTraversal(create().V().out(), false);
    }

    @Test
    public void shouldTraverseV_in() {
        assertTraversal(create().V().in(), false);
    }

    @Test
    public void shouldTraverseV_in_in() {
        assertTraversal(create().V().in(), false);
    }

    @Test
    public void shouldTraverseE() {
        assertTraversal(create().E(), false);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void shouldNotTraverseV_out_addInE() {
        assertTraversal(create().V().as("a").out().addInE("test", "a"), true);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void shouldNotTraverseV_out_addOutE() {
        assertTraversal(create().V().as("a").out().addOutE("test", "a"), true);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void shouldNotTraverseV_In_addInE() {
        assertTraversal(create().V().as("a").in().addInE("test", "a"), true);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void shouldNotTraverseV_In_addOutE() {
        assertTraversal(create().V().as("a").in().addOutE("test", "a"), true);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void shouldNotTraverseV_In_addEXINX() {
        assertTraversal(create().V().as("a").in().addE(Direction.IN, "test", "a"), true);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void shouldNotTraverseV_In_addEXOUTX() {
        assertTraversal(create().V().as("a").in().addE(Direction.OUT, "test", "a"), true);
    }

    private GraphTraversalSource create() {
        return graphProvider.traversal(graph, ReadOnlyStrategy.instance());
    }

    private void assertTraversal(final Traversal t, final boolean hasMutatingStep) {
        try {
            t.asAdmin().applyStrategies();
            if (hasMutatingStep) fail("The strategy should have found a mutating step.");
        } catch (final IllegalStateException ise) {
            if (!hasMutatingStep)
                fail("The traversal should not have failed as there is no mutating step.");
            //else  TODO: Stephen, TraversalVerificationStrategy fails before this as mutating operations are not allowed in OLAP
            //    assertEquals("The provided traversal has a mutating step and thus is not read only", ise.getMessage());
        }
    }
}
