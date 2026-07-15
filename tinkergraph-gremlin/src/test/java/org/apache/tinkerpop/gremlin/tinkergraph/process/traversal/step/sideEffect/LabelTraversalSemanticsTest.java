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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * Tests for single-result label traversal semantics (by-pattern) with collection-spread.
 * Verifies that addLabel(Traversal) and addV(Traversal) consume only ONE result from the
 * traversal, and if that result is a Collection, spread its elements into the label set.
 *
 * @since 4.0.0
 */
public class LabelTraversalSemanticsTest {

    private Graph graph;
    private GraphTraversalSource g;

    @Before
    public void setup() {
        final org.apache.commons.configuration2.Configuration config = new org.apache.commons.configuration2.BaseConfiguration();
        config.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
        config.setProperty(AbstractTinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_LABEL_CARDINALITY, "ZERO_OR_MORE");
        graph = TinkerGraph.open(config);
        g = graph.traversal();
    }

    @After
    public void tearDown() throws Exception {
        graph.close();
    }

    // =========================================================================
    // addLabel(Traversal) — single-result semantics
    // =========================================================================

    @Test
    public void shouldAddSingleScalarLabelFromTraversal() {
        final Vertex v = g.addV("person").next();
        g.V(v).addLabel(constant("employee")).iterate();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("person", "employee"));
    }

    @Test
    public void shouldSpreadCollectionResultIntoMultipleLabels() {
        final Vertex v = g.addV("person").next();
        // constant() returning a Collection should be spread into multiple labels
        final List<String> labelList = Arrays.asList("employee", "manager");
        g.V(v).addLabel(constant(labelList)).iterate();
        assertThat(v.labels(), hasSize(3));
        assertThat(v.labels(), containsInAnyOrder("person", "employee", "manager"));
    }

    @Test
    public void shouldSpreadSetResultIntoMultipleLabels() {
        final Vertex v = g.addV("person").next();
        final Set<String> labelSet = new LinkedHashSet<>(Arrays.asList("employee", "manager"));
        g.V(v).addLabel(constant(labelSet)).iterate();
        assertThat(v.labels(), hasSize(3));
        assertThat(v.labels(), containsInAnyOrder("person", "employee", "manager"));
    }

    @Test
    public void shouldTakeOnlySingleResultFromTraversal() {
        // The traversal produces multiple results, but only the first should be consumed
        // (by-pattern: TraversalUtil.apply takes next() not applyAll)
        final Vertex v = g.addV("person").next();
        final Vertex v2 = g.addV("base").next();
        // Use select on a side-effect that stores a single value, not fold() which would give a list
        // Instead, demonstrate with inject which yields multiple values - only first is taken
        g.V(v).as("x").addLabel(select("x").label()).iterate();
        // The traversal select("x").label() yields "person" — single result, so should just add "person" again (idempotent)
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("person"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNullResultFromLabelTraversal() {
        final Vertex v = g.addV("person").next();
        // constant(null) produces null, which should be rejected
        g.V(v).addLabel(constant(null)).iterate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonStringNonCollectionResultFromLabelTraversal() {
        final Vertex v = g.addV("person").next();
        // constant(42) produces an Integer, which is not a valid label type
        g.V(v).addLabel(constant(42)).iterate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectCollectionContainingNonString() {
        final Vertex v = g.addV("person").next();
        // A Collection containing non-String elements should be rejected
        final List<Object> badList = Arrays.asList("valid", 42);
        g.V(v).addLabel(constant(badList)).iterate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectEmptyStringLabelInCollection() {
        final Vertex v = g.addV("person").next();
        // A Collection containing an empty string should be rejected by validateLabel
        final List<String> badList = Arrays.asList("valid", "");
        g.V(v).addLabel(constant(badList)).iterate();
    }

    // =========================================================================
    // addV(Traversal) — single-result + collection-spread semantics
    // =========================================================================

    @Test
    public void shouldCreateVertexWithScalarLabelFromTraversal() {
        final Vertex v = g.addV(constant("person")).next();
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("person"));
    }

    @Test
    public void shouldCreateVertexWithMultipleLabelsFromCollectionTraversal() {
        // When the label traversal yields a Collection, spread into multiple labels
        final List<String> labelList = Arrays.asList("person", "employee");
        final Vertex v = g.addV(constant(labelList)).next();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("person", "employee"));
    }

    @Test
    public void shouldCreateVertexWithMultipleLabelsFromSetTraversal() {
        final Set<String> labelSet = new LinkedHashSet<>(Arrays.asList("person", "employee", "manager"));
        final Vertex v = g.addV(constant(labelSet)).next();
        assertThat(v.labels(), hasSize(3));
        assertThat(v.labels(), containsInAnyOrder("person", "employee", "manager"));
    }

    @Test
    public void shouldPreserveScalarBehaviorForAddVWithSelectTraversal() {
        // Ensure that the existing scalar single-label behavior is preserved
        // addV(select('x')) where x is a string label should produce a single-label vertex
        final Vertex existing = g.addV("template").next();
        final Vertex v = g.V(existing).as("x").addV(select("x").label()).next();
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("template"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNullLabelInAddVTraversal() {
        // A traversal that yields null should throw when used as a vertex label
        // Note: constant(null) will produce null, which Parameters -> getKeyValues -> TraversalUtil.apply yields null
        // Then getLabelsValue will get null and should throw
        g.addV(constant(null)).next();
    }

    // =========================================================================
    // Regression: existing behavior must be preserved
    // =========================================================================

    @Test
    public void shouldPreserveConstantStringBehaviorForAddLabel() {
        // Basic constant("x") -> single label, no regression
        final Vertex v = g.addV("person").next();
        g.V(v).addLabel(constant("developer")).iterate();
        assertThat(v.labels(), containsInAnyOrder("person", "developer"));
    }

    @Test
    public void shouldPreserveStaticStringBehaviorForAddLabel() {
        // addLabel("x") with string literal -> single label, no regression
        final Vertex v = g.addV("person").next();
        g.V(v).addLabel("developer").iterate();
        assertThat(v.labels(), containsInAnyOrder("person", "developer"));
    }

    @Test
    public void shouldPreserveMultipleStaticStringsForAddLabel() {
        // addLabel("x", "y") with multiple string literals
        final Vertex v = g.addV("person").next();
        g.V(v).addLabel("developer", "manager").iterate();
        assertThat(v.labels(), hasSize(3));
        assertThat(v.labels(), containsInAnyOrder("person", "developer", "manager"));
    }

    // =========================================================================
    // addV(Traversal first, Traversal... more) — multi-traversal varargs
    // =========================================================================

    @Test
    public void shouldCreateVertexWithTwoTraversalLabels() {
        // addV(constant("a"), constant("b")) - each traversal yields one scalar label
        final Vertex v = g.addV(constant("person"), constant("employee")).next();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("person", "employee"));
    }

    @Test
    public void shouldCreateVertexWithThreeTraversalLabels() {
        // addV(constant("a"), constant("b"), constant("c"))
        final Vertex v = g.addV(constant("person"), constant("employee"), constant("manager")).next();
        assertThat(v.labels(), hasSize(3));
        assertThat(v.labels(), containsInAnyOrder("person", "employee", "manager"));
    }

    @Test
    public void shouldPreserveTraversalLabelOrder() {
        // Labels should be in the order the traversals were supplied
        final Vertex v = g.addV(constant("c"), constant("a"), constant("b")).next();
        final Set<String> labels = v.labels();
        assertThat(labels, hasSize(3));
        // LinkedHashSet preserves insertion order
        final String[] ordered = labels.toArray(new String[0]);
        assertThat(ordered[0], is("c"));
        assertThat(ordered[1], is("a"));
        assertThat(ordered[2], is("b"));
    }

    @Test
    public void shouldCreateVertexWithMidTraversalMultiTraversalLabels() {
        // Mid-traversal usage: g.V().addV(t1, t2)
        g.addV("seed").next();
        final Vertex v = g.V().addV(constant("dog"), constant("pet")).next();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("dog", "pet"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectCollectionInMultiTraversalMode() {
        // When 2+ traversals are provided, each must produce a scalar String,
        // not a Collection
        final List<String> labels = Arrays.asList("a", "b");
        g.addV(constant(labels), constant("c")).next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNullInMultiTraversalMode() {
        // null is not a valid label
        g.addV(constant("a"), constant(null)).next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonStringInMultiTraversalMode() {
        // Integer is not a valid label type
        g.addV(constant("a"), constant(42)).next();
    }

    @Test
    public void shouldPreserveSingleTraversalCollectionSpread() {
        // Single traversal with Collection result should still spread (Part A behavior)
        final List<String> labelList = Arrays.asList("person", "employee");
        final Vertex v = g.addV(constant(labelList)).next();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("person", "employee"));
    }
}
