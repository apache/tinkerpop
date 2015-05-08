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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.limit;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.global;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Original spec: https://issues.apache.org/jira/browse/TINKERPOP3-670
 *
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
public abstract class TailTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_V_id_order_tailXglobal_2X();

    public abstract Traversal<Vertex, Object> get_g_V_id_order_tailX2X();

    public abstract Traversal<Vertex, Object> get_g_V_id_order_tailX7X();

    public abstract Traversal<Vertex, List<Object>> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_id_foldX_tailXlocal_2X();

    public abstract Traversal<Vertex, Object> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_id_foldX_tailXlocal_1X();

    public abstract Traversal<Vertex, Object> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_out_asXcX_select_byXT_idX_tailXlocal_2X();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_out_asXcX_select_byXT_idX_tailXlocal_1X();

    /** Scenario: Global scope */
    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_id_order_tailXglobal_2X() {
        final Traversal<Vertex, Object> traversal = get_g_V_id_order_tailXglobal_2X();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList(5, 6), traversal.toList());
    }

    /** Scenario: Default scope is global */
    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_id_order_tailX2X() {
        final Traversal<Vertex, Object> traversal = get_g_V_id_order_tailX2X();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList(5, 6), traversal.toList());
    }

    /** Scenario: Global scope, not enough elements */
    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_id_order_tailX7X() {
        final Traversal<Vertex, Object> traversal = get_g_V_id_order_tailX7X();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), traversal.toList());
    }

    /** Scenario: Local scope, List input, N>1 */
    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_id_foldX_tailXlocal_2X() {
        final Traversal<Vertex, List<Object>> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_id_foldX_tailXlocal_2X();
        printTraversalForm(traversal);
        final Set<List<Object>> expected =
            new HashSet(Arrays.asList(
                            Arrays.asList(4, 5),
                            Arrays.asList(4, 3)));
        final Set<List<Object>> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /** Scenario: Local scope, List input, N=1 */
    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_id_foldX_tailXlocal_1X() {
        final Traversal<Vertex, Object> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_id_foldX_tailXlocal_1X();
        printTraversalForm(traversal);
        final Set<Object> expected = new HashSet(Arrays.asList(5, 3));
        final Set<Object> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /** Scenario: Local scope, List input, N=1, empty input */
    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X() {
        final Traversal<Vertex, Object> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X();
        printTraversalForm(traversal);
        final Set<Object> expected = new HashSet();
        final Set<Object> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /** Scenario: Local scope, Map input, N>1 */
    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_asXaX_out_asXbX_out_asXcX_select_byXT_idX_tailXlocal_2X() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_asXaX_out_asXbX_out_asXcX_select_byXT_idX_tailXlocal_2X();
        printTraversalForm(traversal);
        final Set<Map<String, Object>> expected = new HashSet(makeMapList(2,
                 "b", 4, "c", 5,
                 "b", 4, "c", 3));
        final Set<Map<String, Object>> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /** Scenario: Local scope, Map input, N=1 */
    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_asXaX_out_asXbX_out_asXcX_select_byXT_idX_tailXlocal_1X() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_asXaX_out_asXbX_out_asXcX_select_byXT_idX_tailXlocal_1X();
        printTraversalForm(traversal);
        final Set<Map<String, Object>> expected = new HashSet(makeMapList(1,
                 "c", 5,
                 "c", 3));
        final Set<Map<String, Object>> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends TailTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_id_order_tailXglobal_2X() {
            return g.V().id().order().tail(global, 2);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_id_order_tailX2X() {
            return g.V().id().order().tail(2);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_id_order_tailX7X() {
            return g.V().id().order().tail(7);
        }

        @Override
        public Traversal<Vertex, List<Object>> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_id_foldX_tailXlocal_2X() {
            return g.V().as("a").out().as("a").out().as("a").<List<Object>>select("a").by(unfold().id().fold()).tail(local, 2);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_id_foldX_tailXlocal_1X() {
            return g.V().as("a").out().as("a").out().as("a").<Object>select("a").by(unfold().id().fold()).tail(local, 1);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X() {
            return g.V().as("a").out().as("a").out().as("a").<Object>select("a").by(limit(local, 0)).tail(local, 1);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_out_asXcX_select_byXT_idX_tailXlocal_2X() {
            return g.V().as("a").out().as("b").out().as("c").<Object>select().by(T.id).tail(local, 2);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_out_asXcX_select_byXT_idX_tailXlocal_1X() {
            return g.V().as("a").out().as("b").out().as("c").<Object>select().by(T.id).tail(local, 1);
        }
    }
}
