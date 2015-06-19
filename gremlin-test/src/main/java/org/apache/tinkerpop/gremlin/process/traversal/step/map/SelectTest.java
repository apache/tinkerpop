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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
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

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_select(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, String>> get_g_VX1X_asXaX_outXknowsX_asXbX_select_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_select_byXnameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregateXxX_asXbX_select_byXnameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_select_byXnameX_by_XitX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_select_byXskillX_byXnameX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXname_isXmarkoXX_asXaX_select();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_label_groupCount_asXxX_select();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXpersonX_asXpersonX_mapXbothE_label_groupCountX_asXrelationsX_select();

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_select();

    public abstract Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_asXaX_outXcreatedX_asXaX_select();

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

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_hasXname_markoX_asXbX_asXcX_select_by_byXnameX_byXageX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_select_byXnameX_byXlangX_byXinXcreatedX_valuesXnameX_fold_orderXlocalXX();

    // when labels don't exist

    public abstract Traversal<Vertex, String> get_g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX();

    public abstract Traversal<Vertex, Vertex> get_g_V_asXaX_whereXoutXknowsXX_selectXaX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXknowsX_asXbX_select() {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_select(convertToVertexId("marko"));
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
    public void g_VX1X_asXaX_outXknowsX_asXbX_select_byXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_select_byXnameX(convertToVertexId("marko"));
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
    public void g_V_asXaX_out_asXbX_select_byXnameX() {
        Arrays.asList(
                get_g_V_asXaX_out_asXbX_select_byXnameX(),
                get_g_V_asXaX_out_aggregateXxX_asXbX_select_byXnameX()).forEach(traversal -> {
            printTraversalForm(traversal);
            final List<Map<String, String>> expected = makeMapList(2,
                    "a", "marko", "b", "lop",
                    "a", "marko", "b", "vadas",
                    "a", "marko", "b", "josh",
                    "a", "josh", "b", "ripple",
                    "a", "josh", "b", "lop",
                    "a", "peter", "b", "lop");
            checkResults(expected, traversal);
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_name_order_asXbX_select_byXnameX_byXitX() {
        Arrays.asList(
                get_g_V_asXaX_name_order_asXbX_select_byXnameX_by_XitX()).forEach(traversal -> {
            printTraversalForm(traversal);
            final List<Map<String, String>> expected = makeMapList(2,
                    "a", "marko", "b", "marko",
                    "a", "vadas", "b", "vadas",
                    "a", "josh", "b", "josh",
                    "a", "ripple", "b", "ripple",
                    "a", "lop", "b", "lop",
                    "a", "peter", "b", "peter");
            checkResults(expected, traversal);
        });
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_select_byXskillX_byXnameX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_select_byXskillX_byXnameX();
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
    public void g_V_hasXname_isXmarkoXX_asXaX_select() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXname_isXmarkoXX_asXaX_select();
        printTraversalForm(traversal);
        final List<Map<String, Object>> expected = makeMapList(1, "a", g.V(convertToVertexId("marko")).next());
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_label_groupCount_asXxX_select() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_label_groupCount_asXxX_select();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Object> map1 = traversal.next();
        assertEquals(1, map1.size());
        assertTrue(map1.containsKey("x"));
        final Map<String, Long> map2 = (Map<String, Long>) map1.get("x");
        assertEquals(2, map2.size());
        assertTrue(map2.containsKey("person"));
        assertTrue(map2.containsKey("software"));
        assertEquals(2, map2.get("software").longValue());
        assertEquals(4, map2.get("person").longValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_asXpersonX_mapXbothE_label_groupCountX_asXrelationsX_select() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasLabelXpersonX_asXpersonX_mapXbothE_label_groupCountX_asXrelationsX_select();
        printTraversalForm(traversal);
        final Set<Object> persons = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            assertTrue(traversal.hasNext());
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("person"));
            assertTrue(map.containsKey("relations"));
            assertTrue(persons.add(map.get("person")));
            final Map<String, Long> relations = (Map<String, Long>) map.get("relations");
            if (map.get("person").equals(convertToVertex(graph, "marko"))) {
                assertEquals(2, relations.size());
                assertEquals(1, relations.get("created").longValue());
                assertEquals(2, relations.get("knows").longValue());
            } else if (map.get("person").equals(convertToVertex(graph, "vadas"))) {
                assertEquals(1, relations.size());
                assertEquals(1, relations.get("knows").longValue());
            } else if (map.get("person").equals(convertToVertex(graph, "josh"))) {
                assertEquals(2, relations.size());
                assertEquals(2, relations.get("created").longValue());
                assertEquals(1, relations.get("knows").longValue());
            } else if (map.get("person").equals(convertToVertex(graph, "peter"))) {
                assertEquals(1, relations.size());
                assertEquals(1, relations.get("created").longValue());
            } else {
                throw new IllegalStateException("Unknown vertex result: " + map.get("person"));
            }
        }
        assertFalse(traversal.hasNext());
        assertEquals(4, persons.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_select() {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_select();
        printTraversalForm(traversal);
        int counter = 0;
        int xCounter = 0;
        int yCounter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Vertex> map = traversal.next();
            assertEquals(1, map.size());
            if (map.containsKey("a")) {
                final Vertex vertex = map.get("a");
                xCounter++;
                assertTrue(vertex.equals(convertToVertex(graph, "vadas")) || vertex.equals(convertToVertex(graph, "lop")) || vertex.equals(convertToVertex(graph, "ripple")));
            } else {
                final Vertex vertex = map.get("b");
                yCounter++;
                assertTrue(vertex.equals(convertToVertex(graph, "marko")) || vertex.equals(convertToVertex(graph, "josh")) || vertex.equals(convertToVertex(graph, "peter")));
            }
        }
        assertEquals(6, counter);
        assertEquals(3, yCounter);
        assertEquals(3, xCounter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXcreatedX_asXaX_select() {
        final Traversal<Vertex, Map<String, List<Vertex>>> traversal = get_g_V_asXaX_outXcreatedX_asXaX_select();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, List<Vertex>> map = traversal.next();
            assertEquals(1, map.size());
            final List<Vertex> list = map.get("a");
            assertEquals(2, list.size());
            if (list.get(0).equals(convertToVertex(graph, "marko")))
                assertEquals(convertToVertex(graph, "lop"), list.get(1));
            else if (list.get(0).equals(convertToVertex(graph, "peter")))
                assertEquals(convertToVertex(graph, "lop"), list.get(1));
            else {
                assertEquals(convertToVertex(graph, "josh"), list.get(0));
                assertTrue(convertToVertex(graph, "lop").equals(list.get(1)) || convertToVertex(graph, "ripple").equals(list.get(1)));
            }
        }
        assertEquals(4, counter);
    }

    //

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
        final List<Traversal<Vertex, Edge>> traversals = Arrays.asList(
                get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(convertToVertexId("marko")),
                get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX(convertToVertexId("marko")),
                get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX(convertToVertexId("marko")));
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            assertTrue(traversal.hasNext());
            assertTrue(traversal.hasNext());
            final Edge edge = traversal.next();
            assertEquals("knows", edge.label());
            assertEquals(1.0d, edge.<Double>value("weight"), 0.00001d);
            assertFalse(traversal.hasNext());
            assertFalse(traversal.hasNext());
        });
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
        final List<Traversal<Vertex, Map<String, Long>>> traversals = Arrays.asList(get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            assertTrue(traversal.hasNext());
            final Map<String, Long> map = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(2, map.size());
            assertEquals(1l, map.get("ripple").longValue());
            assertEquals(6l, map.get("lop").longValue());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_hasXname_markoX_asXbX_asXcX_select_by_byXnameX_byXageX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_asXaX_hasXname_markoX_asXbX_asXcX_select_by_byXnameX_byXageX();
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
    public void g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_select_byXnameX_byXlangX_byXinXcreatedX_valuesXnameX_fold_orderXlocalXX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_select_byXnameX_byXlangX_byXinXcreatedX_valuesXnameX_fold_orderXlocalXX();
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

    public static class Traversals extends SelectTest {
        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").select();
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_VX1X_asXaX_outXknowsX_asXbX_select_byXnameX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<String>select().by("name");
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
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_select_byXnameX() {
            return g.V().as("a").out().as("b").<String>select().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregateXxX_asXbX_select_byXnameX() {
            return g.V().as("a").out().aggregate("x").as("b").<String>select().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_select_byXnameX_by_XitX() {
            return g.V().as("a").values("name").order().as("b").<String>select().by("name").by();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_select_byXskillX_byXnameX() {
            return g.V().has("name", "gremlin").inE("uses").order().by("skill", Order.incr).as("a").outV().as("b").select().by("skill").by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXname_isXmarkoXX_asXaX_select() {
            return g.V().has("name", __.is("marko")).as("a").select();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_label_groupCount_asXxX_select() {
            return g.V().label().groupCount().as("x").select();
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXpersonX_asXpersonX_mapXbothE_label_groupCountX_asXrelationsX_select() {
            return g.V().hasLabel("person").as("person").map(__.bothE().label().groupCount()).as("relations").select();
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_select() {
            return g.V().choose(__.outE().count().is(0L), __.as("a"), __.as("b")).select();
        }

        @Override
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_asXaX_outXcreatedX_asXaX_select() {
            return g.V().as("a").out("created").as("a").select();
        }

        //

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
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_hasXname_markoX_asXbX_asXcX_select_by_byXnameX_byXageX() {
            return g.V().as("a").has("name", "marko").as("b").as("c").select().by().by("name").by("age");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_select_byXnameX_byXlangX_byXinXcreatedX_valuesXnameX_fold_orderXlocalXX() {
            return g.V().hasLabel("software").as("name").as("language").as("creators").select().by("name").by("lang").
                    by(__.in("created").values("name").fold().order(local));
        }

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
    }
}
