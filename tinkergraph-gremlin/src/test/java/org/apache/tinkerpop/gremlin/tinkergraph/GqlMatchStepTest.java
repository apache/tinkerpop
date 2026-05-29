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
package org.apache.tinkerpop.gremlin.tinkergraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Unit tests for {@link org.apache.tinkerpop.gremlin.gql.GqlMatchStep} semantics that
 * require a concrete graph and cannot be expressed as Gherkin scenarios.
 */
public class GqlMatchStepTest {

    private TinkerGraph graph;
    private GraphTraversalSource g;

    @Before
    public void setup() {
        graph = TinkerFactory.createModern();
        g = graph.traversal();
    }

    @After
    public void teardown() throws Exception {
        graph.close();
    }

    /**
     * Path-label pre-binding is not yet implemented. When a pattern variable shares its name
     * with a step label already bound in the incoming traverser's path, the step must fail
     * loudly rather than silently producing wrong results (a full unanchored scan). This guard
     * reserves the semantic space so the feature can land as a clean addition later.
     */
    @Test
    public void shouldThrowWhenPatternVariableOverlapsWithStepLabel() {
        try {
            g.V(1).as("a").match("MATCH (a:person)-[:knows]->(b:person)").toList();
            fail("Expected UnsupportedOperationException for path-label/pattern-variable overlap");
        } catch (final UnsupportedOperationException e) {
            // expected — guard fires because 'a' is both a step label and a pattern variable
        }
    }

    @Test
    public void shouldThrowWhenEdgePatternVariableOverlapsWithStepLabel() {
        try {
            g.V(1).outE("knows").as("e").match("MATCH (a:person)-[e:knows]->(b:person)").toList();
            fail("Expected UnsupportedOperationException for path-label/pattern-variable overlap on edge");
        } catch (final UnsupportedOperationException e) {
            // expected — guard fires because 'e' is both a step label and a pattern variable
        }
    }

    @Test
    public void shouldNotThrowWhenStepLabelsAndPatternVariablesAreDisjoint() {
        // 'x' is the step label; pattern uses 'a' and 'b' — no overlap, no guard fires
        g.V(1).as("x").match("MATCH (a:person)-[:knows]->(b:person)").toList();
    }
}
