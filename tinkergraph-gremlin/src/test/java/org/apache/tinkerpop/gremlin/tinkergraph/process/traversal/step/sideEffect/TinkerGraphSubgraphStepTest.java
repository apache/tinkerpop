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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

/**
 * Verifies {@code subgraph()} throws a descriptive {@link IllegalStateException} for non-{@code Edge} input.
 */
public class TinkerGraphSubgraphStepTest {

    @Test
    public void shouldThrowDescriptiveExceptionWhenSubgraphReceivesNonEdgePrimitiveInput() throws Exception {
        try (final TinkerGraph graph = TinkerGraph.open()) {
            final GraphTraversalSource g = graph.traversal();
            try {
                g.inject(1).subgraph("sg").iterate();
                fail("subgraph() should throw an IllegalStateException when the input is not an Edge");
            } catch (IllegalStateException ise) {
                assertThat(ise.getMessage(), containsString("requires Edge input"));
                assertThat(ise.getMessage(), containsString("Integer"));
            }
        }
    }

    @Test
    public void shouldThrowDescriptiveExceptionWhenSubgraphReceivesVertexInput() throws Exception {
        try (final TinkerGraph graph = TinkerGraph.open()) {
            final GraphTraversalSource g = graph.traversal();
            try {
                g.addV().subgraph("sg").iterate();
                fail("subgraph() should throw an IllegalStateException when the input is not an Edge");
            } catch (IllegalStateException ise) {
                assertThat(ise.getMessage(), containsString("requires Edge input"));
            }
        }
    }
}
