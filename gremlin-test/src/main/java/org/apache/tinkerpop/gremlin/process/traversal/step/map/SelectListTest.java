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
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Order;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SelectListTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, List<Vertex>>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectList(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, List<String>>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectList_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, List<Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX(final Object v1Id);

    public abstract Traversal<Vertex, List<String>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_out_asXbX_selectList_byXnameX();

    public abstract Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_out_aggregateXxX_asXbX_selectList_byXnameX();

    public abstract Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_name_order_asXbX_selectList_byXnameX_by_XitX();

    public abstract Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectList_byXskillX_byXnameX();

    public abstract Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasXname_isXmarkoXX_asXaX_selectList();

    public abstract Traversal<Vertex, Map<String, List<Map<String, Long>>>> get_g_V_label_groupCount_asXxX_selectList();

    public abstract Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_asXpersonX_localXbothE_label_groupCountX_asXrelationsX_selectList_byXnameX_by();

    public abstract Traversal<Vertex, List<Vertex>> get_g_V_asXaX_out_asXaX_out_asXaX_selectListXaX();

    public abstract Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectListXaX_byXnameX();

    // below we original back()-tests

    public abstract Traversal<Vertex, List<Vertex>> get_g_VX1X_asXhereX_out_selectListXhereX(final Object v1Id);

    public abstract Traversal<Vertex, List<Vertex>> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX(final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX_unfold_name(final Object v4Id);

    public abstract Traversal<Vertex, List<Edge>> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectListXhereX(final Object v1Id);

    public abstract Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectListXhereX(final Object v1Id);

    public abstract Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectListXhereX(final Object v1Id);

    public abstract Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectListXhereX(final Object v1Id);

    public abstract Traversal<Vertex, List<Vertex>> get_g_V_asXhereXout_name_selectListXhereX();

    public abstract Traversal<Vertex, Map<List<String>, Long>> get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectListXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectListXprojectXX_groupCount_byXnameX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXknowsX_asXbX_selectList() {
        final Traversal<Vertex, Map<String, List<Vertex>>> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_selectList(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, List<Vertex>> bindings = traversal.next();
            assertEquals(2, bindings.size());
            List<Vertex> a = bindings.get("a");
            assertEquals(1, a.size());
            assertEquals(convertToVertexId("marko"), a.get(0).id());
            List<Vertex> b = bindings.get("b");
            assertEquals(1, b.size());
            Vertex b0 = b.get(0);
            assertTrue(b0.id().equals(convertToVertexId("vadas")) || b0.id().equals(convertToVertexId("josh")));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    // @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1X_asXaX_outXknowsX_asXbX_selectList_byXnameX() {
        final Traversal<Vertex, Map<String, List<String>>> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_selectList_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, List<String>> bindings = traversal.next();
            assertEquals(2, bindings.size());
            List<String> a = bindings.get("a");
            assertEquals(1, a.size());
            assertEquals("marko", a.get(0));
            List<String> b = bindings.get("b");
            assertEquals(1, b.size());
            String b0 = b.get(0);
            assertTrue(b0.equals("josh") || b0.equals("vadas"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX() {
        final Traversal<Vertex, List<Vertex>> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            List<Vertex> list = traversal.next();
            assertEquals(1, list.size());
            Vertex vertex = list.get(0);
            assertEquals(convertToVertexId("marko"), vertex.id());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    //@IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX_byXnameX() {
        final Traversal<Vertex, List<String>> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            List<String> list = traversal.next();
            assertEquals(1, list.size());
            assertEquals("marko", list.get(0));
        }
        assertEquals(2, counter);
    }

    private final static <X> List<X> L(final X...elements) { return Arrays.asList(elements); }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_asXaX_out_asXbX_selectList_byXnameX() {
        Arrays.asList(
                get_g_V_asXaX_out_asXbX_selectList_byXnameX(),
                get_g_V_asXaX_out_aggregateXxX_asXbX_selectList_byXnameX()).forEach(traversal -> {
            printTraversalForm(traversal);
            final List<Map<String, List<String>>> expected = makeMapList(2,
                    "a", L("marko"), "b", L("lop"),
                    "a", L("marko"), "b", L("vadas"),
                    "a", L("marko"), "b", L("josh"),
                    "a", L("josh"), "b", L("ripple"),
                    "a", L("josh"), "b", L("lop"),
                    "a", L("peter"), "b", L("lop"));
            checkResults(expected, traversal);
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_asXaX_name_order_asXbX_selectList_byXnameX_byXitX() {
        Arrays.asList(
                get_g_V_asXaX_name_order_asXbX_selectList_byXnameX_by_XitX()).forEach(traversal -> {
            printTraversalForm(traversal);
            final List<Map<String, List<String>>> expected = makeMapList(2,
                    "a", L("marko"), "b", L("marko"),
                    "a", L("vadas"), "b", L("vadas"),
                    "a", L("josh"), "b", L("josh"),
                    "a", L("ripple"), "b", L("ripple"),
                    "a", L("lop"), "b", L("lop"),
                    "a", L("peter"), "b", L("peter"));
            checkResults(expected, traversal);
        });
    }

    @Test
    @LoadGraphWith(CREW)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectList_byXskillX_byXnameX() {
        final Traversal<Vertex, Map<String, List<Object>>> traversal = get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectList_byXskillX_byXnameX();
        printTraversalForm(traversal);
        final List<Map<String, List<Object>>> expected = makeMapList(2,
                   "a", L(3), "b", L("matthias"),
                   "a", L(4), "b", L("marko"),
                   "a", L(5), "b", L("stephen"),
                   "a", L(5), "b", L("daniel"));
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_isXmarkoXX_asXaX_selectList() {
        final Traversal<Vertex, Map<String, List<Object>>> traversal = get_g_V_hasXname_isXmarkoXX_asXaX_selectList();
        printTraversalForm(traversal);
        final List<Map<String, List<Object>>> expected = makeMapList(1, "a", L(g.V(convertToVertexId("marko")).next()));
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_label_groupCount_asXxX_selectList() {
        final Traversal<Vertex, Map<String, List<Map<String, Long>>>> traversal = get_g_V_label_groupCount_asXxX_selectList();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, List<Map<String, Long>>> map1 = traversal.next();
        assertEquals(1, map1.size());
        assertTrue(map1.containsKey("x"));
        final List<Map<String, Long>> list = (List<Map<String, Long>>) map1.get("x");
        final Map<String, Long> map2 = list.get(0);
        assertEquals(2, map2.size());
        assertTrue(map2.containsKey("person"));
        assertTrue(map2.containsKey("software"));
        assertEquals(2, map2.get("software").longValue());
        assertEquals(4, map2.get("person").longValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    @Ignore("Even though the reduce is local, you cannot select across that barrier step")
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_hasLabelXpersonX_asXpersonX_localXbothE_label_groupCountX_asXrelationsX_selectList_byXnameX_by() {
        final Traversal<Vertex, Map<String, List<Object>>> traversal = get_g_V_hasLabelXpersonX_asXpersonX_localXbothE_label_groupCountX_asXrelationsX_selectList_byXnameX_by();
        printTraversalForm(traversal);
        final Set<String> persons = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            assertTrue(traversal.hasNext());
            final Map<String, List<Object>> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("person"));
            assertTrue(map.containsKey("relations"));
            final List<String> personList = (List<String>)(List) map.get("person");
            assertEquals(1, personList.size());
            final String person = personList.get(0);
            assertTrue(persons.add(person));
            final List<Map<String, Long>> relationsList = (List<Map<String, Long>>)(List) map.get("relations");
            assertEquals(1, relationsList.size());
            final Map<String, Long> relations = relationsList.get(0);
            switch (person) {
                case "marko":
                    assertEquals(2, relations.size());
                    assertEquals(1, relations.get("created").longValue());
                    assertEquals(2, relations.get("knows").longValue());
                    break;
                case "vadas":
                    assertEquals(1, relations.size());
                    assertEquals(1, relations.get("knows").longValue());
                    break;
                case "josh":
                    assertEquals(2, relations.size());
                    assertEquals(2, relations.get("created").longValue());
                    assertEquals(1, relations.get("knows").longValue());
                    break;
                case "peter":
                    assertEquals(1, relations.size());
                    assertEquals(1, relations.get("created").longValue());
                    break;
                default:
                    assertTrue(false);
                    break;
            }
        }
        assertFalse(traversal.hasNext());
        assertEquals(4, persons.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectListXaX() {
        final Traversal<Vertex, List<Vertex>> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectListXaX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            List<Vertex> vertices = traversal.next();
            assertEquals(3, vertices.size());
            assertEquals("marko", vertices.get(0).value("name"));
            assertEquals("josh", vertices.get(1).value("name"));
            Vertex last = vertices.get(2);
            assertTrue(last.value("name").equals("ripple") || last.value("name").equals("lop"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXaX_out_asXaX_selectListXaX_byXnameX() {
        final Traversal<Vertex, List<String>> traversal = get_g_V_asXaX_out_asXaX_out_asXaX_selectListXaX_byXnameX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            List<String> names = traversal.next();
            assertEquals(3, names.size());
            assertEquals("marko", names.get(0));
            assertEquals("josh", names.get(1));
            String last = names.get(2);
            assertTrue(last.equals("ripple") || last.equals("lop"));
        }
        assertEquals(2, counter);
    }

    //

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXhereX_out_selectListXhereX() {
        final Traversal<Vertex, List<Vertex>> traversal = get_g_VX1X_asXhereX_out_selectListXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final List<Vertex> list = traversal.next();
            assertEquals(1, list.size());
            assertEquals("marko", list.get(0).value("name"));
        }
        assertEquals(3, counter);
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX() {
        final Traversal<Vertex, List<Vertex>> traversal = get_g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final List<Vertex> list = traversal.next();
            assertEquals(1, list.size());
            final Vertex vertex = list.get(0);
            assertEquals("java", vertex.<String>value("lang"));
            assertTrue(vertex.value("name").equals("ripple") || vertex.value("name").equals("lop"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectListXhereX() {
        final Traversal<Vertex, List<Edge>> traversal = get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectListXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<Edge> list = traversal.next();
        assertEquals(1, list.size());
        final Edge edge = list.get(0);
        assertEquals("knows", edge.label());
        assertEquals(convertToVertexId("vadas"), edge.inVertex().id());
        assertEquals(convertToVertexId("marko"), edge.outVertex().id());
        assertEquals(0.5d, edge.<Double>value("weight"), 0.0001d);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX_unfold_name() {
        final Traversal<Vertex, String> traversal = get_g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX_unfold_name(convertToVertexId("josh"));
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
    public void g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectListXhereX() {
        final List<Traversal<Vertex, List<Edge>>> traversals = Arrays.asList(
                get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectListXhereX(convertToVertexId("marko")),
                get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectListXhereX(convertToVertexId("marko")),
                get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectListXhereX(convertToVertexId("marko")));
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            assertTrue(traversal.hasNext());
            assertTrue(traversal.hasNext());
            final List<Edge> list = traversal.next();
            assertEquals(1, list.size());
            final Edge edge = list.get(0);
            assertEquals("knows", edge.label());
            assertEquals(1.0d, edge.<Double>value("weight"), 0.00001d);
            assertFalse(traversal.hasNext());
            assertFalse(traversal.hasNext());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXhereXout_name_selectListXhereX() {
        Traversal<Vertex, List<Vertex>> traversal = get_g_V_asXhereXout_name_selectListXhereX();
        printTraversalForm(traversal);
        super.checkResults(new HashMap<List<Vertex>, Long>() {{
            put(L(convertToVertex(graph, "marko")), 3l);
            put(L(convertToVertex(graph, "josh")), 2l);
            put(L(convertToVertex(graph, "peter")), 1l);
        }}, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    @Ignore("groupCount needs to be able to apply 'by' traversals to a List")
    public void g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectListXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectListXprojectXX_groupCount_byXnameX() {
        List<Traversal<Vertex, Map<List<String>, Long>>> traversals = Arrays.asList(get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectListXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectListXprojectXX_groupCount_byXnameX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            assertTrue(traversal.hasNext());
            final Map<List<String>, Long> map = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(2, map.size());
            assertEquals(1l, map.get(L("ripple")).longValue());
            assertEquals(6l, map.get(L("lop")).longValue());
        });
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends SelectListTest {
        @Override
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectList(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<Vertex>selectList();
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectList_byXnameX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<String>selectList().by("name");
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<Vertex>selectList("a");
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX_byXnameX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<String>selectList("a").by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_out_asXbX_selectList_byXnameX() {
            return g.V().as("a").out().as("b").<String>selectList().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_out_aggregateXxX_asXbX_selectList_byXnameX() {
            return g.V().as("a").out().aggregate("x").as("b").<String>selectList().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_name_order_asXbX_selectList_byXnameX_by_XitX() {
            return g.V().as("a").values("name").order().as("b").<String>selectList().by("name").by();
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectList_byXskillX_byXnameX() {
            return g.V().has("name", "gremlin").inE("uses").order().by("skill", Order.incr).as("a").outV().as("b").selectList().by("skill").by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasXname_isXmarkoXX_asXaX_selectList() {
            return g.V().has(values("name").is("marko")).as("a").selectList();
        }

        @Override
        public Traversal<Vertex, Map<String, List<Map<String, Long>>>> get_g_V_label_groupCount_asXxX_selectList() {
            return g.V().label().groupCount().as("x").<Map<String, Long>>selectList();
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_asXpersonX_localXbothE_label_groupCountX_asXrelationsX_selectList_byXnameX_by() {
            return g.V().hasLabel("person").as("person").local(__.bothE().label().groupCount()).as("relations").selectList().by("name").by();
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_asXaX_out_asXaX_out_asXaX_selectListXaX() {
            return g.V().as("a").out().as("a").out().as("a").<Vertex>selectList("a");
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectListXaX_byXnameX() {
            return g.V().as("a").out().as("a").out().as("a").<String>selectList("a").by("name");
        }

        //

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_VX1X_asXhereX_out_selectListXhereX(final Object v1Id) {
            return g.V(v1Id).as("here").out().<Vertex>selectList("here");
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX(final Object v4Id) {
            return g.V(v4Id).out().as("here").has("lang", "java").<Vertex>selectList("here");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX_unfold_name(final Object v4Id) {
            return g.V(v4Id).out().as("here").has("lang", "java").<Vertex>selectList("here").unfold().values("name");
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectListXhereX(final Object v1Id) {
            return g.V(v1Id).outE().as("here").inV().has("name", "vadas").<Edge>selectList("here");
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectListXhereX(final Object v1Id) {
            return g.V(v1Id).outE("knows").has("weight", 1.0d).as("here").inV().has("name", "josh").<Edge>selectList("here");
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectListXhereX(final Object v1Id) {
            return g.V(v1Id).outE("knows").as("here").has("weight", 1.0d).inV().has("name", "josh").<Edge>selectList("here");
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectListXhereX(final Object v1Id) {
            return g.V(v1Id).outE("knows").as("here").has("weight", 1.0d).as("fake").inV().has("name", "josh").<Edge>selectList("here");
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_asXhereXout_name_selectListXhereX() {
            return g.V().as("here").out().values("name").<Vertex>selectList("here");
        }

        @Override
        public Traversal<Vertex, Map<List<String>, Long>> get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectListXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectListXprojectXX_groupCount_byXnameX() {
            return (Traversal) g.V().out("created")
                .union(as("project").in("created").has("name", "marko").<Vertex>selectList("project"),
                       as("project").in("created").in("knows").has("name", "marko").<Vertex>selectList("project"))
                .<String>groupCount().by("name");
        }
    }
}
