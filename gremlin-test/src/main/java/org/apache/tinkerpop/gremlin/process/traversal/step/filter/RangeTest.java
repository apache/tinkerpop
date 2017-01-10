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
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class RangeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X();

    public abstract Traversal<Vertex, List<String>> get_g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_2X();

    public abstract Traversal<Vertex, String> get_g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_1X();

    public abstract Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_3X();

    public abstract Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X();

    public abstract Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_4_5X();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2X();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_1X();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_3X();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_2X();

    public abstract Traversal<Vertex, String> get_g_V_hasLabelXpersonX_order_byXageX_valuesXnameX_skipX1X();

    public abstract Traversal<Vertex, String> get_g_V_hasLabelXpersonX_order_byXageX_skipX1X_valuesXnameX();

    public abstract Traversal<Vertex, List<Double>> get_g_V_outE_valuesXweightX_fold_orderXlocalX_skipXlocal_2X();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_limitX2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_limitX2X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXoutE_limitX1X_inVX_limitX3X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_localXoutE_limitX1X_inVX_limitX3X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(3, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXbothX_timesX3X_rangeX5_11X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_repeatXbothX_timesX3X_rangeX5_11X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            counter++;
        }
        assertEquals(6, counter);
    }

    /**
     * Scenario: limit step, Scope.local, >1 item requested, List<String> input, List<String> output
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_2X() {
        final Traversal<Vertex, List<String>> traversal = get_g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_2X();
        printTraversalForm(traversal);
        final Set<List<String>> expected =
                new HashSet(Arrays.asList(
                        Arrays.asList("ripple", "josh"),
                        Arrays.asList("lop", "josh")));
        final Set<List<String>> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /**
     * Scenario: limit step, Scope.local, 1 item requested, List input, String output
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_1X() {
        final Traversal<Vertex, String> traversal = get_g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_1X();
        printTraversalForm(traversal);
        final Set<String> expected = new HashSet<>(Arrays.asList("ripple", "lop"));
        final Set<List<String>> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /**
     * Scenario: range step, Scope.local, >1 item requested, List<String> input, List<String> output
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_3X() {
        final Traversal<Vertex, List<String>> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_3X();
        printTraversalForm(traversal);
        final Set<List<String>> expected =
                new HashSet<>(Arrays.asList(
                        Arrays.asList("josh", "ripple"),
                        Arrays.asList("josh", "lop")));
        final Set<List<String>> actual = new HashSet(traversal.toList());
        assertEquals(expected, actual);
    }

    /**
     * Scenario: range step, Scope.local, 1 item requested, List input, String output
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X() {
        final Traversal<Vertex, String> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final String s = traversal.next();
            assertEquals("josh", s);
            counter++;
        }
        assertEquals(2, counter);
    }

    /**
     * Scenario: range step, Scope.local, 1 item requested, List input, no items selected, stop traversal
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_4_5X() {
        final Traversal<Vertex, String> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_4_5X();
        printTraversalForm(traversal);
        assertEquals(Arrays.asList(), traversal.toList());
    }

    /**
     * Scenario: limit step, Scope.local, >1 item requested, Map input, Map output
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2X() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2X();
        printTraversalForm(traversal);
        final Set<Map<String, String>> expected = new HashSet<>(makeMapList(2,
                "a", "ripple", "b", "josh",
                "a", "lop", "b", "josh"));
        final Set<Map<String, String>> actual = new HashSet<>(traversal.toList());
        assertEquals(expected, actual);
    }

    /**
     * Scenario: limit step, Scope.local, 1 item requested, Map input, Map output
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_1X() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_1X();
        printTraversalForm(traversal);
        final Set<Map<String, String>> expected = new HashSet<>(makeMapList(1,
                "a", "ripple",
                "a", "lop"));
        final Set<Map<String, String>> actual = new HashSet<>(traversal.toList());
        assertEquals(expected, actual);
    }

    /**
     * Scenario: range step, Scope.local, >1 item requested, Map input, Map output
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_3X() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_3X();
        printTraversalForm(traversal);
        final Set<Map<String, String>> expected = new HashSet<>(makeMapList(2,
                "b", "josh", "c", "ripple",
                "b", "josh", "c", "lop"));
        final Set<Map<String, String>> actual = new HashSet<>(traversal.toList());
        assertEquals(expected, actual);
    }

    /**
     * Scenario: range step, Scope.local, 1 item requested, Map input, Map output
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_2X() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_2X();
        printTraversalForm(traversal);
        // Since both of the tuples are identical, we count them.
        final Set<Map<String, String>> expected = new HashSet<>(makeMapList(1, "b", "josh"));
        final Set<Map<String, String>> actual = new HashSet<>();
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, String> map = traversal.next();
            actual.add(map);
            counter++;
        }
        assertEquals(2, counter);
        assertEquals(expected, actual);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_order_byXageX_valuesXnameX_skipX1X() {
        final Traversal<Vertex, String> traversal = get_g_V_hasLabelXpersonX_order_byXageX_valuesXnameX_skipX1X();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(Arrays.asList("marko", "josh", "peter"), traversal.toList());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_order_byXageX_skipX1X_valuesXnameX() {
        final Traversal<Vertex, String> traversal = get_g_V_hasLabelXpersonX_order_byXageX_skipX1X_valuesXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(Arrays.asList("marko", "josh", "peter"), traversal.toList());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_valuesXweightX_fold_orderXlocalX_skipXlocal_2X() {
        final Traversal<Vertex, List<Double>> traversal = get_g_V_outE_valuesXweightX_fold_orderXlocalX_skipXlocal_2X();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(Arrays.asList(0.4, 0.5, 1.0, 1.0), traversal.next());
        assertFalse(traversal.hasNext());
    }

    public static class Traversals extends RangeTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id) {
            return g.V(v1Id).out().limit(2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X() {
            return g.V().local(outE().limit(1)).inV().limit(3);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            return g.V(v1Id).out("knows").outE("created").range(0, 1).inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            return g.V(v1Id).out("knows").out("created").range(0, 1);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            return g.V(v1Id).out("created").in("created").range(1, 3);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            return g.V(v1Id).out("created").inE("created").range(1, 3).outV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X() {
            return g.V().repeat(both()).times(3).range(5, 11);
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_2X() {
            return g.V().as("a").in().as("a").in().as("a").<List<String>>select(Pop.mixed, "a").by(unfold().values("name").fold()).limit(local, 2);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_in_asXaX_in_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_limitXlocal_1X() {
            return g.V().as("a").in().as("a").in().as("a").<List<String>>select(Pop.mixed, "a").by(unfold().values("name").fold()).limit(local, 1);
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_3X() {
            return g.V().as("a").out().as("a").out().as("a").<List<String>>select(Pop.mixed, "a").by(unfold().values("name").fold()).range(local, 1, 3);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X() {
            return g.V().as("a").out().as("a").out().as("a").<List<String>>select(Pop.mixed, "a").by(unfold().values("name").fold()).range(local, 1, 2);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_rangeXlocal_4_5X() {
            return g.V().as("a").out().as("a").out().as("a").<List<String>>select(Pop.mixed, "a").by(unfold().values("name").fold()).range(local, 4, 5);
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2X() {
            return g.V().as("a").in().as("b").in().as("c").<Map<String, String>>select("a", "b", "c").by("name").limit(local, 2);
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_1X() {
            return g.V().as("a").in().as("b").in().as("c").<Map<String, String>>select("a", "b", "c").by("name").limit(local, 1);
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_3X() {
            return g.V().as("a").out().as("b").out().as("c").<Map<String, String>>select("a", "b", "c").by("name").range(local, 1, 3);
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_2X() {
            return g.V().as("a").out().as("b").out().as("c").<Map<String, String>>select("a", "b", "c").by("name").range(local, 1, 2);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasLabelXpersonX_order_byXageX_valuesXnameX_skipX1X() {
            return g.V().hasLabel("person").order().by("age").<String>values("name").skip(1);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasLabelXpersonX_order_byXageX_skipX1X_valuesXnameX() {
            return g.V().hasLabel("person").order().by("age").skip(1).values("name");
        }

        @Override
        public Traversal<Vertex, List<Double>> get_g_V_outE_valuesXweightX_fold_orderXlocalX_skipXlocal_2X() {
            return g.V().outE().values("weight").fold().order(local).skip(local, 2);
        }
    }
}
