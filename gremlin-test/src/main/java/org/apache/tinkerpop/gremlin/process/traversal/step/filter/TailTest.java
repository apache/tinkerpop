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
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
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
 * Original spec: https://issues.apache.org/jira/browse/TINKERPOP-670
 *
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class TailTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailXglobal_2X();

    public abstract Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailX2X();

    public abstract Traversal<Vertex, String> get_g_V_valuesXnameX_order_tail();

    public abstract Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailX7X();

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_tailX7X();

    public abstract Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_2X();

    public abstract Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_1X();

    public abstract Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocalX();

    public abstract Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_2X();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_1X();

    /** Scenario: Global scope */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valuesXnameX_order_tailXglobal_2X() {
        final Traversal<Vertex, String> traversal = get_g_V_valuesXnameX_order_tailXglobal_2X();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList("ripple", "vadas"), traversal.toList());
    }

    /** Scenario: Default scope is global */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valuesXnameX_order_tailX2X() {
        final Traversal<Vertex, String> traversal = get_g_V_valuesXnameX_order_tailX2X();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList("ripple", "vadas"), traversal.toList());
    }

    /** Scenario: Default is global, N=1 */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valuesXnameX_order_tail() {
        final Traversal<Vertex, String> traversal = get_g_V_valuesXnameX_order_tail();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList("vadas"), traversal.toList());
    }

    /** Scenario: Global scope, not enough elements */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valuesXnameX_order_tailX7X() {
        final Traversal<Vertex, String> traversal = get_g_V_valuesXnameX_order_tailX7X();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList("josh", "lop", "marko", "peter", "ripple", "vadas"), traversal.toList());
    }

    /** Scenario: Global scope, using repeat (BULK) */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXbothX_timesX3X_tailX7X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_repeatXbothX_timesX3X_tailX7X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            counter++;
        }
        assertEquals(7, counter);
    }

    /** Scenario: Local scope, List input, N>1 */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_2X() {
        final Traversal<Vertex, List<String>> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_2X();
        printTraversalForm(traversal);
        final Set<List<String>> expected =
            new HashSet(Arrays.asList(
                            Arrays.asList("josh", "ripple"),
                            Arrays.asList("josh", "lop")));
        final Set<List<String>> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /** Scenario: Local scope, List input, N=1 */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_1X() {
        final Traversal<Vertex, String> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_1X();
        printTraversalForm(traversal);
        final Set<String> expected = new HashSet(Arrays.asList("ripple", "lop"));
        final Set<String> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /** Scenario: Local scope, List input, default N=1 */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocalX() {
        final Traversal<Vertex, String> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocalX();
        printTraversalForm(traversal);
        final Set<String> expected = new HashSet(Arrays.asList("ripple", "lop"));
        final Set<String> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /** Scenario: Local scope, List input, N=1, empty input */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X() {
        final Traversal<Vertex, String> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X();
        printTraversalForm(traversal);
        final Set<String> expected = new HashSet();
        final Set<String> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /** Scenario: Local scope, Map input, N>1 */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_2X() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_2X();
        printTraversalForm(traversal);
        final Set<Map<String, String>> expected = new HashSet(makeMapList(2,
                 "b", "josh", "c", "ripple",
                 "b", "josh", "c", "lop"));
        final Set<Map<String, String>> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /** Scenario: Local scope, Map input, N=1 */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_1X() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_1X();
        printTraversalForm(traversal);
        final Set<Map<String, String>> expected = new HashSet(makeMapList(1,
                 "c", "ripple",
                 "c", "lop"));
        final Set<Map<String, String>> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    public static class Traversals extends TailTest {

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailXglobal_2X() {
            return g.V().<String>values("name").order().tail(global, 2);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailX2X() {
            return g.V().<String>values("name").order().tail(2);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tail() {
            return g.V().<String>values("name").order().tail();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailX7X() {
            return g.V().<String>values("name").order().tail(7);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_tailX7X() {
            return g.V().repeat(both()).times(3).tail(7);
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_2X() {
            return g.V().as("a").out().as("a").out().as("a").<List<String>>select("a").by(unfold().values("name").fold()).tail(local, 2);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_1X() {
            return g.V().as("a").out().as("a").out().as("a").<String>select("a").by(unfold().values("name").fold()).tail(local, 1);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocalX() {
            return g.V().as("a").out().as("a").out().as("a").<String>select("a").by(unfold().values("name").fold()).tail(local);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X() {
            return g.V().as("a").out().as("a").out().as("a").<String>select("a").by(limit(local, 0)).tail(local, 1);
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_2X() {
            return g.V().as("a").out().as("b").out().as("c").<String>select("a","b","c").by("name").tail(local, 2);
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_1X() {
            return g.V().as("a").out().as("b").out().as("c").<String>select("a","b","c").by("name").tail(local, 1);
        }
    }
}
