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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.apache.tinkerpop.gremlin.structure.Column.values;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class SelectTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, String>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_selectXa_bX_byXnameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_isXmarkoXX_asXaX_selectXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_label_groupCount_asXxX_selectXxX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX();

    public abstract Traversal<Vertex, Vertex> get_g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX();

    // below are original back()-tests

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_asXhereX_out_selectXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX(final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name(final Object v4Id);

    public abstract Traversal<Vertex, Edge> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_asXhereXout_name_selectXhereX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_hasXname_markoX_asXbX_asXcX_selectXa_b_cX_by_byXnameX_byXageX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX();

    // TINKERPOP-619: select should not throw

    public abstract Traversal<Vertex, Object> get_g_V_selectXaX(final Pop pop);

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_selectXa_bX(final Pop pop);

    public abstract Traversal<Vertex, Object> get_g_V_valueMap_selectXpop_aX(final Pop pop);

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_valueMap_selectXpop_a_bX(final Pop pop);

    // when labels don't exist

    public abstract Traversal<Vertex, String> get_g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX();

    public abstract Traversal<Vertex, Vertex> get_g_V_asXaX_whereXoutXknowsXX_selectXaX();

    // select column tests

    public abstract Traversal<Vertex, Long> get_g_V_outE_weight_groupCount_selectXvaluesX_unfold();

    public abstract Traversal<Vertex, Long> get_g_V_outE_weight_groupCount_unfold_selectXvaluesX_unfold();

    public abstract Traversal<Vertex, Long> get_g_V_outE_weight_groupCount_selectXvaluesX_unfold_groupCount_selectXvaluesX_unfold();

    public abstract Traversal<Vertex, Double> get_g_V_outE_weight_groupCount_selectXkeysX_unfold();

    public abstract Traversal<Vertex, Double> get_g_V_outE_weight_groupCount_unfold_selectXkeysX_unfold();

    public abstract Traversal<Vertex, Collection<Set<String>>> get_g_V_asXa_bX_out_asXcX_path_selectXkeysX();

    // Useful for permuting Pop use cases
    protected final static List<Pop> POPS = Arrays.asList(null, Pop.first, Pop.last, Pop.all);

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX() {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Vertex> bindings = traversal.next();
            assertEquals(2, bindings.size());
            assertEquals(convertToVertexId("marko"), (bindings.get("a")).id());
            assertTrue((bindings.get("b")).id().equals(convertToVertexId("vadas")) || bindings.get("b").id().equals(convertToVertexId("josh")));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, String> bindings = traversal.next();
            assertEquals(2, bindings.size());
            assertEquals("marko", bindings.get("a"));
            assertTrue(bindings.get("b").equals("josh") || bindings.get("b").equals("vadas"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXknowsX_asXbX_selectXaX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            assertEquals(convertToVertexId("marko"), vertex.id());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("marko", traversal.next());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXbX_selectXa_bX_byXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_out_asXbX_selectXa_bX_byXnameX();
        printTraversalForm(traversal);
        assertCommonA(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX();
        printTraversalForm(traversal);
        assertCommonA(traversal);
    }

    private void assertCommonA(final Traversal<Vertex, Map<String, String>> traversal) {
        final List<Map<String, String>> expected = makeMapList(2,
                "a", "marko", "b", "lop",
                "a", "marko", "b", "vadas",
                "a", "marko", "b", "josh",
                "a", "josh", "b", "ripple",
                "a", "josh", "b", "lop",
                "a", "peter", "b", "lop");
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX();
        printTraversalForm(traversal);
        final List<Map<String, String>> expected = makeMapList(2,
                "a", "marko", "b", "marko",
                "a", "vadas", "b", "vadas",
                "a", "josh", "b", "josh",
                "a", "ripple", "b", "ripple",
                "a", "lop", "b", "lop",
                "a", "peter", "b", "peter");
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX();
        printTraversalForm(traversal);
        final List<Map<String, Object>> expected = makeMapList(2,
                "a", 3, "b", "matthias",
                "a", 4, "b", "marko",
                "a", 5, "b", "stephen",
                "a", 5, "b", "daniel");
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_isXmarkoXX_asXaX_selectXaX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_isXmarkoXX_asXaX_selectXaX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(convertToVertex(graph, "marko")), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_label_groupCount_asXxX_selectXxX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_label_groupCount_asXxX_selectXxX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Long> map1 = traversal.next();
        assertEquals(2, map1.size());
        assertTrue(map1.containsKey("person"));
        assertTrue(map1.containsKey("software"));
        assertEquals(2, map1.get("software").longValue());
        assertEquals(4, map1.get("person").longValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX();
        printTraversalForm(traversal);
        final Set<Object> persons = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            assertTrue(traversal.hasNext());
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("p"));
            assertTrue(map.containsKey("r"));
            assertTrue(persons.add(map.get("p")));
            final Map<String, Long> relations = (Map<String, Long>) map.get("r");
            if (map.get("p").equals(convertToVertex(graph, "marko"))) {
                assertEquals(2, relations.size());
                assertEquals(1, relations.get("created").longValue());
                assertEquals(2, relations.get("knows").longValue());
            } else if (map.get("p").equals(convertToVertex(graph, "vadas"))) {
                assertEquals(1, relations.size());
                assertEquals(1, relations.get("knows").longValue());
            } else if (map.get("p").equals(convertToVertex(graph, "josh"))) {
                assertEquals(2, relations.size());
                assertEquals(2, relations.get("created").longValue());
                assertEquals(1, relations.get("knows").longValue());
            } else if (map.get("p").equals(convertToVertex(graph, "peter"))) {
                assertEquals(1, relations.size());
                assertEquals(1, relations.get("created").longValue());
            } else {
                throw new IllegalStateException("Unknown vertex result: " + map.get("p"));
            }
        }
        assertFalse(traversal.hasNext());
        assertEquals(4, persons.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX();
        printTraversalForm(traversal);
        int counter = 0;
        int xCounter = 0;
        int yCounter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            if (vertex.equals(convertToVertex(graph, "vadas")) || vertex.equals(convertToVertex(graph, "lop")) || vertex.equals(convertToVertex(graph, "ripple")))
                xCounter++;
            else if (vertex.equals(convertToVertex(graph, "marko")) || vertex.equals(convertToVertex(graph, "josh")) || vertex.equals(convertToVertex(graph, "peter")))
                yCounter++;
            else
                fail("This state should not have occurred");
        }
        assertEquals(6, counter);
        assertEquals(3, yCounter);
        assertEquals(3, xCounter);
    }

    // below are original back()-tests

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXhereX_out_selectXhereX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_asXhereX_out_selectXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("marko", traversal.next().<String>value("name"));
        }
        assertEquals(3, counter);
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            assertEquals("java", vertex.<String>value("lang"));
            assertTrue(vertex.value("name").equals("ripple") || vertex.value("name").equals("lop"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertEquals("knows", edge.label());
        assertEquals(convertToVertexId("vadas"), edge.inVertex().id());
        assertEquals(convertToVertexId("marko"), edge.outVertex().id());
        assertEquals(0.5d, edge.<Double>value("weight"), 0.0001d);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            names.add(traversal.next());
        }
        assertEquals(2, counter);
        assertEquals(2, names.size());
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("lop"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertCommonB(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertCommonB(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertCommonB(traversal);
    }

    private static void assertCommonB(final Traversal<Vertex, Edge> traversal) {
        assertTrue(traversal.hasNext());
        assertTrue(traversal.hasNext());
        final Edge edge = traversal.next();
        assertEquals("knows", edge.label());
        assertEquals(1.0d, edge.<Double>value("weight"), 0.00001d);
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXhereXout_name_selectXhereX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_asXhereXout_name_selectXhereX();
        printTraversalForm(traversal);
        super.checkResults(new HashMap<Vertex, Long>() {{
            put(convertToVertex(graph, "marko"), 3l);
            put(convertToVertex(graph, "josh"), 2l);
            put(convertToVertex(graph, "peter"), 1l);
        }}, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX() {
        Traversal<Vertex, Map<String, Long>> traversal = get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(1l, map.get("ripple").longValue());
        assertEquals(6l, map.get("lop").longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_hasXname_markoX_asXbX_asXcX_selectXa_b_cX_by_byXnameX_byXageX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_asXaX_hasXname_markoX_asXbX_asXcX_selectXa_b_cX_by_byXnameX_byXageX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Object> map = traversal.next();
        assertEquals(3, map.size());
        assertEquals(g.V(convertToVertexId("marko")).next(), map.get("a"));
        assertEquals("marko", map.get("b"));
        assertEquals(29, map.get("c"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX();
        printTraversalForm(traversal);
        for (int i = 0; i < 2; i++) {
            assertTrue(traversal.hasNext());
            final Map<String, Object> map = traversal.next();
            assertEquals(3, map.size());
            final List<String> creators = (List<String>) map.get("creators");
            final boolean isLop = "lop".equals(map.get("name")) && "java".equals(map.get("language")) &&
                    creators.size() == 3 && creators.get(0).equals("josh") && creators.get(1).equals("marko") && creators.get(2).equals("peter");
            final boolean isRipple = "ripple".equals(map.get("name")) && "java".equals(map.get("language")) &&
                    creators.size() == 1 && creators.get(0).equals("josh");
            assertTrue(isLop ^ isRipple);
        }
        assertFalse(traversal.hasNext());
    }

    // TINKERPOP-619: select should not throw

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_selectXaX() {
        POPS.forEach(pop -> {
            final Traversal<Vertex, Object> traversal = get_g_V_selectXaX(pop);
            printTraversalForm(traversal);
            assertEquals(Collections.emptyList(), traversal.toList());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_selectXa_bX() {
        POPS.forEach(pop -> {
            final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_selectXa_bX(pop);
            printTraversalForm(traversal);
            assertEquals(Collections.emptyList(), traversal.toList());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMap_selectXpop_aX() {
        POPS.forEach(pop -> {
            final Traversal<Vertex, Object> traversal = get_g_V_valueMap_selectXpop_aX(pop);
            printTraversalForm(traversal);
            assertEquals(Collections.emptyList(), traversal.toList());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMap_selectXpop_a_bX() {
        POPS.forEach(pop -> {
            final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_valueMap_selectXpop_a_bX(pop);
            printTraversalForm(traversal);
            assertEquals(Collections.emptyList(), traversal.toList());
        });
    }

    // when labels don't exist

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX() {
        final Traversal<Vertex, String> traversal = get_g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX();
        printTraversalForm(traversal);
        checkResults(this.<String, Long>makeMapList(1, "marko", 5l).get(0), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX();
        printTraversalForm(traversal);
        Map<String, String> result = traversal.next();
        assertEquals(2, result.size());
        assertEquals("josh", result.get("a"));
        assertEquals("marko", result.get("b"));
        result = traversal.next();
        assertEquals(2, result.size());
        assertEquals("josh", result.get("a"));
        assertEquals("marko", result.get("b"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_whereXoutXknowsXX_selectXaX_byXnameX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_asXaX_whereXoutXknowsXX_selectXaX();
        printTraversalForm(traversal);
        assertEquals(convertToVertex(graph, "marko"), traversal.next());
        assertFalse(traversal.hasNext());
    }

    //////

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_weight_groupCount_selectXkeysX_unfold() {
        final Traversal<Vertex, Double> traversal = get_g_V_outE_weight_groupCount_selectXkeysX_unfold();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(0.2, 0.4, 0.5, 1.0), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_weight_groupCount_unfold_selectXkeysX_unfold() {
        final Traversal<Vertex, Double> traversal = get_g_V_outE_weight_groupCount_unfold_selectXkeysX_unfold();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(0.2, 0.4, 0.5, 1.0), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_weight_groupCount_selectXvaluesX_unfold() {
        final Traversal<Vertex, Long> traversal = get_g_V_outE_weight_groupCount_selectXvaluesX_unfold();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(1l, 1l, 2l, 2l), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_weight_groupCount_unfold_selectXvaluesX_unfold() {
        final Traversal<Vertex, Long> traversal = get_g_V_outE_weight_groupCount_unfold_selectXvaluesX_unfold();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(1l, 1l, 2l, 2l), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_weight_groupCount_selectXvaluesX_unfold_groupCount_selectXvaluesX_unfold() {
        final Traversal<Vertex, Long> traversal = get_g_V_outE_weight_groupCount_selectXvaluesX_unfold_groupCount_selectXvaluesX_unfold();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(2l, 2l), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXa_bX_out_asXcX_selectXkeysX() {
        final Traversal<Vertex, Collection<Set<String>>> traversal = get_g_V_asXa_bX_out_asXcX_path_selectXkeysX();
        int counter = 0;
        while (traversal.hasNext()) {
            final List<Set<String>> set = (List) traversal.next();
            assertTrue(set.get(0).contains("a"));
            assertTrue(set.get(0).contains("b"));
            assertEquals(2, set.get(0).size());
            assertTrue(set.get(1).contains("c"));
            assertEquals(1, set.get(1).size());
            counter++;
        }
        assertEquals(6, counter);
        assertFalse(traversal.hasNext());
    }

    public static class Traversals extends SelectTest {
        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").select("a", "b");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<String>select("a", "b").by("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").select("a");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<String>select("a").by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_selectXa_bX_byXnameX() {
            return g.V().as("a").out().as("b").<String>select("a", "b").by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX() {
            return g.V().as("a").out().aggregate("x").as("b").<String>select("a", "b").by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX() {
            return g.V().as("a").values("name").order().as("b").<String>select("a", "b").by("name").by();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX() {
            return g.V().has("name", "gremlin").inE("uses").order().by("skill", Order.incr).as("a").outV().as("b").select("a", "b").by("skill").by("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_isXmarkoXX_asXaX_selectXaX() {
            return g.V().has("name", __.is("marko")).as("a").select("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_label_groupCount_asXxX_selectXxX() {
            return g.V().label().groupCount().as("x").select("x");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX() {
            return g.V().hasLabel("person").as("p").map(__.bothE().label().groupCount()).as("r").select("p", "r");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX() {
            return g.V().choose(__.outE().count().is(0L), __.as("a"), __.as("b")).choose(__.select("a"), __.select("a"), __.select("b"));
        }

        // below are original back()-tests

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXhereX_out_selectXhereX(final Object v1Id) {
            return g.V(v1Id).as("here").out().select("here");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX(final Object v4Id) {
            return g.V(v4Id).out().as("here").has("lang", "java").select("here");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name(final Object v4Id) {
            return g.V(v4Id).out().as("here").has("lang", "java").select("here").values("name");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX(final Object v1Id) {
            return g.V(v1Id).outE().as("here").inV().has("name", "vadas").select("here");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(final Object v1Id) {
            return g.V(v1Id).outE("knows").has("weight", 1.0d).as("here").inV().has("name", "josh").select("here");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX(final Object v1Id) {
            return g.V(v1Id).outE("knows").as("here").has("weight", 1.0d).inV().has("name", "josh").<Edge>select("here");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX(final Object v1Id) {
            return g.V(v1Id).outE("knows").as("here").has("weight", 1.0d).as("fake").inV().has("name", "josh").<Edge>select("here");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXhereXout_name_selectXhereX() {
            return g.V().as("here").out().values("name").select("here");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX() {
            return (Traversal) g.V().out("created")
                    .union(as("project").in("created").has("name", "marko").select("project"),
                            as("project").in("created").in("knows").has("name", "marko").select("project")).groupCount().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_hasXname_markoX_asXbX_asXcX_selectXa_b_cX_by_byXnameX_byXageX() {
            return g.V().as("a").has("name", "marko").as("b").as("c").select("a", "b", "c").by().by("name").by("age");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX() {
            return g.V().hasLabel("software").as("name").as("language").as("creators").select("name", "language", "creators").by("name").by("lang").
                    by(__.in("created").values("name").fold().order(local));
        }

        // TINKERPOP-619: select should not throw

        @Override
        public Traversal<Vertex, Object> get_g_V_selectXaX(final Pop pop) {
            final GraphTraversal<Vertex, Vertex> root = g.V();
            return null == pop ? root.select("a") : root.select(pop, "a");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_selectXa_bX(final Pop pop) {
            final GraphTraversal<Vertex, Vertex> root = g.V();
            return null == pop ? root.select("a", "b") : root.select(pop, "a", "b");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_valueMap_selectXpop_aX(final Pop pop) {
            final GraphTraversal<Vertex, Map<String, Object>> root = g.V().valueMap();
            return null == pop ? root.select("a") : root.select(pop, "a");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_valueMap_selectXpop_a_bX(final Pop pop) {
            final GraphTraversal<Vertex, Map<String, Object>> root = g.V().valueMap();
            return null == pop ? root.select("a", "b") : root.select(pop, "a", "b");
        }

        // when labels don't exist

        @Override
        public Traversal<Vertex, String> get_g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX() {
            return g.V().until(__.out().out()).repeat(__.in().as("a")).<String>select("a").by(__.tail(local).values("name"));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX() {
            return g.V().until(__.out().out()).repeat(__.in().as("a").in().as("b")).<String>select("a", "b").by("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_whereXoutXknowsXX_selectXaX() {
            return g.V().as("a").where(__.out("knows")).<Vertex>select("a");
        }

        // select columns test

        @Override
        public Traversal<Vertex, Long> get_g_V_outE_weight_groupCount_selectXvaluesX_unfold() {
            return g.V().outE().values("weight").groupCount().select(values).unfold();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_outE_weight_groupCount_unfold_selectXvaluesX_unfold() {
            return g.V().outE().values("weight").groupCount().unfold().select(values).unfold();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_outE_weight_groupCount_selectXvaluesX_unfold_groupCount_selectXvaluesX_unfold() {
            return g.V().outE().values("weight").groupCount().select(values).unfold().groupCount().select(values).unfold();
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_outE_weight_groupCount_selectXkeysX_unfold() {
            return g.V().outE().values("weight").groupCount().select(keys).unfold();
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_outE_weight_groupCount_unfold_selectXkeysX_unfold() {
            return g.V().outE().values("weight").groupCount().unfold().select(keys).unfold();
        }

        @Override
        public Traversal<Vertex, Collection<Set<String>>> get_g_V_asXa_bX_out_asXcX_path_selectXkeysX() {
            return g.V().as("a", "b").out().as("c").path().select(keys);
        }
    }
}
