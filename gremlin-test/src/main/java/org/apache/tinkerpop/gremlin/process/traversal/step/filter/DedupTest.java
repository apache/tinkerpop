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
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.dedup;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.structure.Column.values;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class DedupTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_both_dedup_name();

    public abstract Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name();

    public abstract Traversal<Vertex, String> get_g_V_both_name_order_byXa_bX_dedup_value();

    public abstract Traversal<Vertex, String> get_g_V_both_both_name_dedup();

    public abstract Traversal<Vertex, Vertex> get_g_V_both_both_dedup();

    public abstract Traversal<Vertex, Vertex> get_g_V_both_both_dedup_byXlabelX();

    public abstract Traversal<Vertex, Map<String, List<Double>>> get_g_V_group_byXlabelX_byXbothE_weight_dedup_foldX();

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX();

    public abstract Traversal<Vertex, Path> get_g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path();

    public abstract Traversal<Vertex, String> get_g_V_outE_asXeX_inV_asXvX_selectXeX_order_byXweight_ascX_selectXvX_valuesXnameX_dedup();

    public abstract Traversal<Vertex, String> get_g_V_both_both_dedup_byXoutE_countX_name();

    public abstract Traversal<Vertex, String> get_g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_out_asXxX_in_asXyX_selectXx_yX_byXnameX_fold_dedupXlocal_x_yX_unfold();

    public abstract Traversal<Vertex, Long> get_g_V_groupCount_selectXvaluesX_unfold_dedup();

    public abstract Traversal<Vertex, Collection<String>> get_g_V_asXaX_repeatXbothX_timesX3X_emit_name_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_foldX_selectXvaluesX_unfold_dedup();

    public abstract Traversal<Vertex, Long> get_g_V_repeatXdedupX_timesX2X_count();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold() {
        final Traversal<Vertex, String> traversal = get_g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh", "peter"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_asXxX_in_asXyX_selectXx_yX_byXnameX_fold_dedupXlocal_x_yX_unfold() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_out_asXxX_in_asXyX_selectXx_yX_byXnameX_fold_dedupXlocal_x_yX_unfold();
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "x", "lop", "y", "marko",
                "x", "lop", "y", "josh",
                "x", "lop", "y", "peter",
                "x", "vadas", "y", "marko",
                "x", "josh", "y", "marko",
                "x", "ripple", "y", "josh"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_dedup_name() {
        final Traversal<Vertex, String> traversal = get_g_V_both_dedup_name();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(6, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("peter"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(1, names.size());
        assertTrue(names.contains("lop") || names.contains("ripple"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_name_order_byXa_bX_dedup_value() {
        final Traversal<Vertex, String> traversal = get_g_V_both_name_order_byXa_bX_dedup_value();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(6, names.size());
        assertEquals("josh", names.get(0));
        assertEquals("lop", names.get(1));
        assertEquals("marko", names.get(2));
        assertEquals("peter", names.get(3));
        assertEquals("ripple", names.get(4));
        assertEquals("vadas", names.get(5));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_both_name_dedup() {
        final Traversal<Vertex, String> traversal = get_g_V_both_both_name_dedup();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "vadas", "josh", "peter", "lop", "ripple"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_both_dedup() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_both_both_dedup();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(
                convertToVertex(graph, "marko"),
                convertToVertex(graph, "vadas"),
                convertToVertex(graph, "josh"),
                convertToVertex(graph, "peter"),
                convertToVertex(graph, "lop"),
                convertToVertex(graph, "ripple")), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_both_dedup_byXlabelX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_both_both_dedup_byXlabelX();
        printTraversalForm(traversal);
        final List<Vertex> vertices = traversal.toList();
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXlabelX_byXbothE_weight_dedup_foldX() {
        final Traversal<Vertex, Map<String, List<Double>>> traversal =
                get_g_V_group_byXlabelX_byXbothE_weight_dedup_foldX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, List<Double>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(3, map.get("software").size());
        assertEquals(4, map.get("person").size());
        assertTrue(map.get("software").contains(0.2));
        assertTrue(map.get("software").contains(0.4));
        assertTrue(map.get("software").contains(1.0));
        //
        assertTrue(map.get("person").contains(0.2));
        assertTrue(map.get("person").contains(0.4));
        assertTrue(map.get("person").contains(0.5));
        assertTrue(map.get("person").contains(1.0));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX() {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX();
        printTraversalForm(traversal);
        int personPersonCounter = 0;
        int personSoftwareCounter = 0;
        int softwarePersonCounter = 0;
        while (traversal.hasNext()) {
            final Map<String, Vertex> map = traversal.next();
            assertEquals(2, map.size());
            if (map.get("a").label().equals("person") && map.get("b").label().equals("person"))
                personPersonCounter++;
            else if (map.get("a").label().equals("person") && map.get("b").label().equals("software"))
                personSoftwareCounter++;
            else if (map.get("a").label().equals("software") && map.get("b").label().equals("person"))
                softwarePersonCounter++;
            else
                fail("Bad result type: " + map);
        }
        assertEquals(1, personPersonCounter);
        assertEquals(1, personSoftwareCounter);
        assertEquals(1, softwarePersonCounter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<List<Vertex>> results = new HashSet<>();
        while (traversal.hasNext()) {
            final Path path = traversal.next();
            assertEquals(3, path.size());
            assertTrue(results.add(Arrays.asList(path.get("a"), path.get("b"))));
            counter++;
        }
        assertEquals(4, counter);
        assertEquals(4, results.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_asXeX_inV_asXvX_selectXeX_order_byXweight_ascX_selectXvX_valuesXnameX_dedup() {
        final Traversal<Vertex, String> traversal = get_g_V_outE_asXeX_inV_asXvX_selectXeX_order_byXweight_ascX_selectXvX_valuesXnameX_dedup();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(4, names.size());
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("ripple"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_both_dedup_byXoutE_countX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_both_both_dedup_byXoutE_countX_name();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(4, names.size());
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("marko"));
        // the 4th is vadas, ripple, or lop
        assertEquals(4, new HashSet<>(names).size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupCount_selectXvaluesX_unfold_dedup() {
        final Traversal<Vertex, Long> traversal = get_g_V_groupCount_selectXvaluesX_unfold_dedup();
        printTraversalForm(traversal);
        checkResults(Collections.singletonList(1L), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_repeatXbothX_timesX3X_emit_name_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_foldX_selectXvaluesX_unfold_dedup() {
        final Traversal<Vertex, Collection<String>> traversal = get_g_V_asXaX_repeatXbothX_timesX3X_emit_name_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_foldX_selectXvaluesX_unfold_dedup();
        printTraversalForm(traversal);
        final List<String> vertices = new ArrayList<>(traversal.next());
        assertFalse(traversal.hasNext());
        assertEquals(6, vertices.size());
        assertEquals("josh", vertices.get(0));
        assertEquals("lop", vertices.get(1));
        assertEquals("marko", vertices.get(2));
        assertEquals("peter", vertices.get(3));
        assertEquals("ripple", vertices.get(4));
        assertEquals("vadas", vertices.get(5));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXdedupX_timesX2X_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_repeatXdedupX_timesX2X_count();
        printTraversalForm(traversal);
        assertEquals(0L, traversal.next().longValue());
        assertFalse(traversal.hasNext());
    }


    public static class Traversals extends DedupTest {
        @Override
        public Traversal<Vertex, String> get_g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold() {
            return g.V().out().in().values("name").fold().dedup(Scope.local).unfold();
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_out_asXxX_in_asXyX_selectXx_yX_byXnameX_fold_dedupXlocal_x_yX_unfold() {
            return g.V().out().as("x").in().as("y").select("x", "y").by("name").fold().dedup(Scope.local, "x", "y").unfold();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            return g.V().both().dedup().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
            return g.V().both().has(T.label, "software").dedup().by("lang").values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_name_order_byXa_bX_dedup_value() {
            return g.V().both().<String>properties("name").order().by((a, b) -> a.value().compareTo(b.value())).dedup().value();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_both_name_dedup() {
            return g.V().both().both().<String>values("name").dedup();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_both_both_dedup() {
            return g.V().both().both().dedup();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_both_both_dedup_byXlabelX() {
            return g.V().both().both().dedup().by(T.label);
        }

        @Override
        public Traversal<Vertex, Map<String, List<Double>>> get_g_V_group_byXlabelX_byXbothE_weight_dedup_foldX() {
            return g.V().<String, List<Double>>group().by(T.label).by(bothE().values("weight").dedup().fold());
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX() {
            return g.V().as("a").both().as("b").dedup("a", "b").by(T.label).select("a", "b");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path() {
            return g.V().as("a").out("created").as("b").in("created").as("c").dedup("a", "b").path();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outE_asXeX_inV_asXvX_selectXeX_order_byXweight_ascX_selectXvX_valuesXnameX_dedup() {
            return g.V().outE().as("e").inV().as("v").select("e").order().by("weight", Order.asc).select("v").<String>values("name").dedup();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_both_dedup_byXoutE_countX_name() {
            return g.V().both().both().dedup().by(__.outE().count()).values("name");
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_groupCount_selectXvaluesX_unfold_dedup() {
            return g.V().groupCount().select(values).<Long>unfold().dedup();
        }

        @Override
        public Traversal<Vertex, Collection<String>> get_g_V_asXaX_repeatXbothX_timesX3X_emit_name_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_foldX_selectXvaluesX_unfold_dedup() {
            return g.V().as("a").repeat(both()).times(3).emit().values("name").as("b").group().by(select("a")).by(select("b").dedup().order().fold()).select(values).<Collection<String>>unfold().dedup();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXdedupX_timesX2X_count() {
            return g.V().repeat(dedup()).times(2).count();
        }
    }
}
