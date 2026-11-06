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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for vertex label cardinality modes: ONE, ONE_OR_MORE, and ZERO_OR_MORE.
 */
public class LabelCardinalityTest {

    // ========================================================================
    // ONE mode (TinkerGraph default)
    // ========================================================================

    @Test
    public void oneMode_addVNoLabel_shouldDefaultToVertex() throws Exception {
        final Graph graph = TinkerGraph.open();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV().next();
            assertThat(v.labels(), hasSize(1));
            assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL));
        } finally {
            graph.close();
        }
    }

    @Test
    public void oneMode_addVWithLabel_shouldHaveThatLabel() throws Exception {
        final Graph graph = TinkerGraph.open();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            assertThat(v.labels(), hasSize(1));
            assertThat(v.labels(), containsInAnyOrder("person"));
        } finally {
            graph.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void oneMode_addVWithMultipleLabels_shouldThrow() throws Exception {
        final Graph graph = TinkerGraph.open();
        try {
            final GraphTraversalSource g = graph.traversal();
            g.addV("a", "b").next();
        } finally {
            graph.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void oneMode_addLabel_shouldThrow() throws Exception {
        final Graph graph = TinkerGraph.open();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            v.addLabel("x");
        } finally {
            graph.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void oneMode_dropLabel_shouldThrow() throws Exception {
        final Graph graph = TinkerGraph.open();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            v.dropLabel(Vertex.DEFAULT_LABEL);
        } finally {
            graph.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void oneMode_dropLabels_shouldThrow() throws Exception {
        final Graph graph = TinkerGraph.open();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            v.dropLabels();
        } finally {
            graph.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void oneMode_addLabelViaTraversal_shouldThrow() throws Exception {
        final Graph graph = TinkerGraph.open();
        try {
            final GraphTraversalSource g = graph.traversal();
            g.addV("person").addLabel("x").iterate();
        } finally {
            graph.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void oneMode_dropLabelViaTraversal_shouldThrow() throws Exception {
        final Graph graph = TinkerGraph.open();
        try {
            final GraphTraversalSource g = graph.traversal();
            g.addV("person").dropLabel("person").iterate();
        } finally {
            graph.close();
        }
    }

    // ========================================================================
    // ONE_OR_MORE mode
    // ========================================================================

    private Graph openOneOrMore() {
        final Configuration config = new BaseConfiguration();
        config.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
        config.setProperty(AbstractTinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_LABEL_CARDINALITY, "ONE_OR_MORE");
        return TinkerGraph.open(config);
    }

    @Test
    public void oneOrMoreMode_addVNoLabel_shouldContainDefault() throws Exception {
        final Graph graph = openOneOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV().next();
            assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL));
        } finally {
            graph.close();
        }
    }

    @Test
    public void oneOrMoreMode_addVWithLabel_shouldContainDefaultAndUserLabel() throws Exception {
        final Graph graph = openOneOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL, "person"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void oneOrMoreMode_addLabel_shouldAccumulate() throws Exception {
        final Graph graph = openOneOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            v.addLabel("employee");
            assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL, "person", "employee"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void oneOrMoreMode_dropLabel_shouldSucceedIfAtLeastOneRemains() throws Exception {
        final Graph graph = openOneOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            // Has {"vertex", "person"}  - dropping "person" leaves {"vertex"}
            v.dropLabel("person");
            assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL));
        } finally {
            graph.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void oneOrMoreMode_dropLabel_shouldThrowIfWouldLeaveZero() throws Exception {
        final Graph graph = openOneOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV().next();
            // Has {"vertex"}  - dropping "vertex" would leave 0
            v.dropLabel(Vertex.DEFAULT_LABEL);
        } finally {
            graph.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void oneOrMoreMode_dropMultipleLabels_shouldThrowIfWouldLeaveZero() throws Exception {
        final Graph graph = openOneOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            // Has {"vertex", "person"}  - dropping both would leave 0
            v.dropLabel("person", Vertex.DEFAULT_LABEL);
        } finally {
            graph.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void oneOrMoreMode_dropLabels_shouldAlwaysThrow() throws Exception {
        final Graph graph = openOneOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            v.dropLabels();
        } finally {
            graph.close();
        }
    }

    @Test
    public void oneOrMoreMode_addVWithMultipleLabels_shouldIncludeDefault() throws Exception {
        final Graph graph = openOneOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("a", "b").next();
            assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL, "a", "b"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void oneOrMoreMode_addDuplicateLabel_shouldBeNoOp() throws Exception {
        final Graph graph = openOneOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            // Has {"vertex", "person"}. Adding "person" again should be no-op.
            v.addLabel("person");
            assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL, "person"));
            assertThat(v.labels(), hasSize(2));
        } finally {
            graph.close();
        }
    }

    @Test
    public void oneOrMoreMode_dropNonExistentLabel_shouldBeNoOp() throws Exception {
        final Graph graph = openOneOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV("person").next();
            // Has {"vertex", "person"}. Dropping "nonexistent" should be no-op.
            v.dropLabel("nonexistent");
            assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL, "person"));
            assertThat(v.labels(), hasSize(2));
        } finally {
            graph.close();
        }
    }

    // ========================================================================
    // ZERO_OR_MORE mode
    // ========================================================================

    private Graph openZeroOrMore() {
        final Configuration config = new BaseConfiguration();
        config.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
        config.setProperty(AbstractTinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_LABEL_CARDINALITY, "ZERO_OR_MORE");
        return TinkerGraph.open(config);
    }

    @Test
    public void zeroOrMoreMode_labelSingular_shouldReturnEmptyStringForNoLabels() throws Exception {
        final Graph graph = openZeroOrMore();
        try {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v = g.addV().next();
            assertThat(v.labels(), is(empty()));
            // label() (singular, deprecated) should return "" not null
            assertThat(v.label(), isString(""));
        } finally {
            graph.close();
        }
    }

    // helper for static import of Matchers.is and empty
    private static org.hamcrest.Matcher<java.util.Collection<?>> is(org.hamcrest.Matcher<java.util.Collection<?>> matcher) {
        return org.hamcrest.Matchers.is(matcher);
    }

    private static org.hamcrest.Matcher<java.util.Collection<?>> empty() {
        return org.hamcrest.Matchers.empty();
    }

    private static org.hamcrest.Matcher<String> isString(final String expected) {
        return org.hamcrest.Matchers.is(expected);
    }
}
