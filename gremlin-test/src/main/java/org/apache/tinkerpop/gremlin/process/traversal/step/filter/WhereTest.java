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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.not;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.and;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.not;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.or;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class WhereTest extends AbstractGremlinProcessTest {

    /// where(local)

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX();

    public abstract Traversal<Vertex, String> get_g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name();

    /// where(global)

    public abstract Traversal<Vertex, String> get_g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name(final Object v1Id);

    public abstract Traversal<Vertex, Object> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX(final Object v1Id);

    // except/retain functionality

    public abstract Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_whereXnotXwithinXaXXX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_withSideEffectXa_graph_verticesX2XX_VX1X_out_whereXneqXaXX(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path(final Object v1Id);

    // hasNot functionality

    public abstract Traversal<Vertex, String> get_g_V_whereXnotXoutXcreatedXXX_name();

    // complex and/or functionality

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX();

    public abstract Traversal<Vertex, String> get_g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_valuesXnameX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX();

    // multi-labels

    //public abstract Traversal<Vertex, String> get_g_V_asXaX_outXknowsX_asXbX_whereXasXa__bX_outXcreatedX_hasXname__rippleX_name();

    // where()-by

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX();

    public abstract Traversal<Vertex, String> get_g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.containsKey("b"));
            assertEquals(map.get("a"), map.get("b"));
        }
        assertEquals(6, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.containsKey("b"));
            assertNotEquals(map.get("a"), map.get("b"));
            assertTrue(((Vertex) map.get("a")).id().equals(convertToVertexId("marko")) ||
                    ((Vertex) map.get("a")).id().equals(convertToVertexId("peter")) ||
                    ((Vertex) map.get("a")).id().equals(convertToVertexId("josh")));
            assertTrue(((Vertex) map.get("b")).id().equals(convertToVertexId("marko")) ||
                    ((Vertex) map.get("b")).id().equals(convertToVertexId("peter")) ||
                    ((Vertex) map.get("b")).id().equals(convertToVertexId("josh")));
        }
        assertEquals(6, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX();
        printTraversalForm(traversal);
        int counter = 0;
        int markoCounter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.containsKey("b"));
            assertEquals(convertToVertexId("marko"), ((Vertex) map.get("b")).id());
            if (((Vertex) map.get("a")).id().equals(convertToVertexId("marko")))
                markoCounter++;
            else
                assertTrue(((Vertex) map.get("a")).id().equals(convertToVertexId("josh")) || ((Vertex) map.get("a")).id().equals(convertToVertexId("peter")));
        }
        assertEquals(3, markoCounter);
        assertEquals(5, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.containsKey("b"));
            assertEquals(convertToVertexId("marko"), ((Vertex) map.get("a")).id());
            assertEquals(convertToVertexId("josh"), ((Vertex) map.get("b")).id());
        }
        assertEquals(1, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh", "peter", "josh"), traversal);
    }

    /// where(global)

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX() {
        final Traversal<Vertex, String> traversal = get_g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX(convertToVertexId(graph, "marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "peter"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name(convertToVertexId(graph, "marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "peter"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX() {
        final Traversal<Vertex, Object> traversal = get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX(convertToVertexId(graph, "marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", 32), traversal);
    }

    /// except/retain functionality

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("peter", "josh"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_aggregateXxX_out_whereXnotXwithinXaXXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_aggregateXxX_out_whereXnotXwithinXaXXX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals("ripple", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSideEffectXa_g_VX2XX_VX1X_out_whereXneqXaXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_withSideEffectXa_graph_verticesX2XX_VX1X_out_whereXneqXaXX(convertToVertexId("marko"), convertToVertex("vadas"));
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("josh") || vertex.value("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<Path> paths = traversal.toList();
        assertEquals(4, paths.size());
        assertEquals(1, paths.stream().filter(path -> path.size() == 3).count());
        assertEquals(2, paths.stream().filter(path -> path.size() == 5).count());
        assertEquals(1, paths.stream().filter(path -> path.size() == 7).count());
        assertFalse(traversal.hasNext());
    }

    // hasNot functionality

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_whereXnotXoutXcreatedXXX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_whereXnotXoutXcreatedXXX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("vadas", "lop", "ripple"), traversal);
    }

    // complex and/or functionality

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX();
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "a", convertToVertex(graph, "marko"), "b", convertToVertex(graph, "josh"),
                "a", convertToVertex(graph, "marko"), "b", convertToVertex(graph, "vadas")), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_valuesXnameX() {
        final Traversal<Vertex, String> traversal = get_g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_valuesXnameX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "vadas", "josh"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX();
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "a", convertToVertex(graph, "marko"), "b", convertToVertex(graph, "lop"),
                "a", convertToVertex(graph, "peter"), "b", convertToVertex(graph, "lop")), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX();
        printTraversalForm(traversal);
        checkResults(makeMapList(4,
                "a", convertToVertex(graph, "marko"), "b", convertToVertex(graph, "lop"), "c", convertToVertex(graph, "josh"), "d", convertToVertex(graph, "vadas"),
                "a", convertToVertex(graph, "peter"), "b", convertToVertex(graph, "lop"), "c", convertToVertex(graph, "josh"), "d", convertToVertex(graph, "vadas")), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX();
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "a", convertToVertex(graph, "marko"), "b", convertToVertex(graph, "josh"),
                "a", convertToVertex(graph, "marko"), "b", convertToVertex(graph, "lop"),
                "a", convertToVertex(graph, "peter"), "b", convertToVertex(graph, "lop"),
                "a", convertToVertex(graph, "josh"), "b", convertToVertex(graph, "lop")), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX();
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "a", "peter", "b", "josh",
                "a", "peter", "b", "marko",
                "a", "josh", "b", "marko"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX();
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "a", "peter", "c", "lop",
                "a", "josh", "c", "lop",
                "a", "josh", "c", "ripple",
                "a", "marko", "c", "lop"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX();
        printTraversalForm(traversal);
        checkResults(makeMapList(3,
                "a", "peter", "c", "lop", "d", "josh",
                "a", "peter", "c", "lop", "d", "marko",
                "a", "josh", "c", "lop", "d", "marko",
                "a", "josh", "c", "lop", "d", "peter"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name(convertToVertexId(graph, "marko"));
        printTraversalForm(traversal);
        assertEquals("josh", traversal.next());
        assertFalse(traversal.hasNext());
    }


    public static class Traversals extends WhereTest {

        /// where(local)

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where("a", eq("b"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where("a", neq("b"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where(as("b").has("name", "marko"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where(as("a").out("knows").as("b"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name() {
            return g.V().as("a").out("created").where(as("a").values("name").is("josh")).in("created").values("name");
        }

        /// where(global)

        @Override
        public Traversal<Vertex, String> get_g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX(final Object v1Id) {
            return g.withSideEffect("a", Arrays.asList("josh", "peter")).V(v1Id).out("created").in("created").<String>values("name").where(P.within("a"));
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").in("created").as("b").where("a", neq("b")).values("name");
        }

        @Override
        public Traversal<Vertex, Object> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").in("created").as("b").where(as("b").out("created").has("name", "ripple")).values("age", "name");
        }

        // except/retain functionality

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").in("created").where(eq("a")).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").in("created").where(neq("a")).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_whereXnotXwithinXaXXX(final Object v1Id) {
            return g.V(v1Id).out().aggregate("x").out().where(not(within("x")));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_withSideEffectXa_graph_verticesX2XX_VX1X_out_whereXneqXaXX(final Object v1Id, final Object v2) {
            return g.withSideEffect("a", v2).V(v1Id).out().where(neq("a"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path(final Object v1Id) {
            return g.V(v1Id).repeat(bothE("created").where(without("e")).aggregate("e").otherV()).emit().path();
        }

        // hasNot functionality

        @Override
        public Traversal<Vertex, String> get_g_V_whereXnotXoutXcreatedXXX_name() {
            return g.V().where(not(out("created"))).values("name");
        }

        // complex and/or functionality

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX() {
            return g.V().as("a").out().as("b").where(and(as("a").out("knows").as("b"), or(as("b").out("created").has("name", "ripple"), as("b").in("knows").count().is(not(eq(0)))))).select("a", "b");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_valuesXnameX() {
            return g.V().where(out("created").and().out("knows").or().in("knows")).values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX() {
            return g.V().as("a").out("created").as("b").where(and(as("b").in(), not(as("a").out("created").has("name", "ripple")))).select("a", "b");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX() {
            return g.V().as("a").out("created").as("b").in("created").as("c").both("knows").both("knows").as("d").where("c", P.not(P.eq("a").or(P.eq("d")))).select("a", "b", "c", "d");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX() {
            return g.V().as("a").out().as("b").where(as("b").in().count().is(eq(3)).or().where(as("b").out("created").and().as("b").has(T.label, "person"))).select("a", "b");
        }

        // where()-by

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX() {
            return g.V().as("a").out("created").in("created").as("b").where("a", gt("b")).by("age").<String>select("a", "b").by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX() {
            return g.V().as("a").outE("created").as("b").inV().as("c").where("a", gt("b").or(eq("b"))).by("age").by("weight").by("weight").<String>select("a", "c").by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX() {
            return g.V().as("a").outE("created").as("b").inV().as("c").in("created").as("d").where("a", lt("b").or(gt("c")).and(neq("d"))).by("age").by("weight").by(in("created").values("age").min()).<String>select("a", "c", "d").by("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name(final Object v1Id) {
            return g.V(v1Id).as("a").out().has("age").where(gt("a")).by("age").values("name");
        }
    }
}