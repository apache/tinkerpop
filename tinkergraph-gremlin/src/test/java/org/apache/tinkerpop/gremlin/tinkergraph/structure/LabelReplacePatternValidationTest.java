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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

/**
 * Validation tests for label replace/swap/clear workaround patterns
 * with multi-label support on TinkerGraph (ZERO_OR_MORE vertex cardinality).
 *
 * These tests validate that common user patterns for replacing labels work
 * correctly in the context of the append-only onMatch T.label semantics.
 */
public class LabelReplacePatternValidationTest {

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

    // =====================================================
    // Pattern 1: Replace all labels via post-merge chaining
    // =====================================================

    @Test
    public void pattern1_replaceAllLabelsPostMerge() {
        // Setup: vertex with labels [person, employee]
        g.addV("person").property("name", "marko").addLabel("employee").iterate();

        // Verify initial state
        final Set<String> before = g.V().has("name", "marko").next().labels();
        assertThat(before, hasSize(2));
        assertThat(before, containsInAnyOrder("person", "employee"));

        // Verify mergeV can find it
        final Vertex merged = g.mergeV(new LinkedHashMap<>(Map.of("name", "marko"))).next();
        assertEquals("marko", merged.value("name"));

        // Pattern: dropLabels() then addLabel() after merge
        g.V().has("name", "marko").dropLabels().addLabel("manager").iterate();

        // Verify result
        final Set<String> after = g.V().has("name", "marko").next().labels();
        assertThat(after, hasSize(1));
        assertThat(after, containsInAnyOrder("manager"));
    }

    // =====================================================
    // Pattern 2: Replace specific label
    // =====================================================

    @Test
    public void pattern2_replaceSpecificLabel() {
        // Setup: vertex with labels [person, employee]
        g.addV("person").property("name", "josh").addLabel("employee").iterate();

        // Verify initial state
        final Set<String> before = g.V().has("name", "josh").next().labels();
        assertThat(before, hasSize(2));
        assertThat(before, containsInAnyOrder("person", "employee"));

        // Pattern: dropLabel("employee") then addLabel("manager")
        g.V().has("name", "josh").dropLabel("employee").addLabel("manager").iterate();

        // Verify result: person remains, employee replaced with manager
        final Set<String> after = g.V().has("name", "josh").next().labels();
        assertThat(after, hasSize(2));
        assertThat(after, containsInAnyOrder("person", "manager"));
    }

    // =====================================================
    // Pattern 3: Clear all labels
    // =====================================================

    @Test
    public void pattern3_clearAllLabels() {
        // Setup: vertex with label [person]
        g.addV("person").property("name", "vadas").iterate();

        // Verify initial state
        final Set<String> before = g.V().has("name", "vadas").next().labels();
        assertThat(before, hasSize(1));
        assertThat(before, containsInAnyOrder("person"));

        // Pattern: dropLabels() to clear all
        g.V().has("name", "vadas").dropLabels().iterate();

        // Verify result: empty label set (ZERO_OR_MORE cardinality allows this)
        final Set<String> after = g.V().has("name", "vadas").next().labels();
        assertThat(after, empty());
    }

    // =====================================================
    // Pattern 4: mergeV then replace labels on match
    // =====================================================

    @Test
    public void pattern4_mergeVThenReplaceLabelsOnMatch() {
        // Setup: vertex with label [person]
        g.addV("person").property("name", "marko").iterate();

        // Verify initial state
        final Set<String> before = g.V().has("name", "marko").next().labels();
        assertThat(before, hasSize(1));
        assertThat(before, containsInAnyOrder("person"));

        // Pattern: mergeV finds the vertex, then chain dropLabels().addLabel()
        final Map<Object, Object> mergeMap = new LinkedHashMap<>();
        mergeMap.put(T.label, "person");
        mergeMap.put("name", "marko");
        g.mergeV(mergeMap).dropLabels().addLabel("manager").iterate();

        // Verify result
        final Set<String> after = g.V().has("name", "marko").next().labels();
        assertThat(after, hasSize(1));
        assertThat(after, containsInAnyOrder("manager"));
    }

    // =====================================================
    // Pattern 5: onMatch with T.label is APPEND-ONLY
    // =====================================================

    @Test
    public void pattern5_onMatchTLabelAppendsOnly() {
        // Setup: vertex with label [person]
        g.addV("person").property("name", "alex").iterate();

        // onMatch with T.label adds to existing labels, does NOT replace
        final Map<Object, Object> mergeMap = new LinkedHashMap<>();
        mergeMap.put(T.label, "person");
        mergeMap.put("name", "alex");

        final Map<Object, Object> onMatchMap = new LinkedHashMap<>();
        onMatchMap.put(T.label, "manager");

        g.mergeV(mergeMap).option(Merge.onMatch, onMatchMap).iterate();

        // Result: labels are [person, manager]  - "manager" was appended
        final Set<String> after = g.V().has("name", "alex").next().labels();
        assertThat(after, hasSize(2));
        assertThat(after, containsInAnyOrder("person", "manager"));
    }

    // =====================================================
    // =====================================================
    // Pattern 8: mergeV + dropLabels + addLabel with sideEffect pattern
    // (label replace using sideEffect within the traversal)
    // =====================================================

    @Test
    public void pattern8_labelReplaceViaSideEffectInTraversal() {
        // Setup: vertex with labels [person, employee]
        g.addV("person").property("name", "dana").addLabel("employee").iterate();

        // Pattern: use sideEffect to perform mutation inline
        g.V().has("name", "dana").sideEffect(__.dropLabels()).addLabel("manager").iterate();

        // Verify result
        final Set<String> after = g.V().has("name", "dana").next().labels();
        assertThat(after, hasSize(1));
        assertThat(after, containsInAnyOrder("manager"));
    }

    // =====================================================
    // Pattern 9: Conditional label replace using choose()
    // =====================================================

    @Test
    public void pattern9_conditionalLabelReplace() {
        // Setup: vertex with labels [person, temp_worker]
        g.addV("person").property("name", "bob").addLabel("temp_worker").iterate();

        // Pattern: if has "temp_worker" label, swap to "permanent"
        g.V().has("name", "bob").
                choose(__.hasLabel("temp_worker"),
                        __.dropLabel("temp_worker").addLabel("permanent")).
                iterate();

        // Verify result: person kept, temp_worker -> permanent
        final Set<String> after = g.V().has("name", "bob").next().labels();
        assertThat(after, hasSize(2));
        assertThat(after, containsInAnyOrder("person", "permanent"));
    }

    // =====================================================
    // Pattern 10: onMatch with sideEffect for label drop+add
    // (demonstrates that sideEffect traversal in onMatch can
    //  mutate labels, but the Map returned still uses append)
    // =====================================================

    @Test
    public void pattern10_onMatchSideEffectForLabelMutation() {
        // Setup: vertex with labels [person, employee]
        g.addV("person").property("name", "eve").addLabel("employee").iterate();

        // The onMatch option traversal can use sideEffect to mutate labels
        // but the final Map returned (empty here) doesn't touch labels
        final Map<Object, Object> mergeMap = new LinkedHashMap<>();
        mergeMap.put(T.label, "person");
        mergeMap.put("name", "eve");

        final Map<String, Object> emptyMatch = new LinkedHashMap<>();

        g.withSideEffect("m", emptyMatch).
                mergeV(mergeMap).
                option(Merge.onMatch, __.sideEffect(__.dropLabels().addLabel("manager")).select("m")).
                iterate();

        // Verify: labels should be [manager] because sideEffect ran drop+add
        final Set<String> after = g.V().has("name", "eve").next().labels();
        assertThat(after, hasSize(1));
        assertThat(after, containsInAnyOrder("manager"));
    }
}
