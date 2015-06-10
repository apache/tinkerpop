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
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class WhereTest extends AbstractGremlinProcessTest {

    /// where(local)

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eqXbXX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neqXbXX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX();

    /// where(global)

    public abstract Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name(final Object v1Id);

    public abstract Traversal<Vertex, Object> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX(final Object v1Id);

    // except/retain functionality

    public abstract Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_whereXwithoutXaXX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_withSideEffectXa_graph_verticesX2XX_VX1X_out_whereXneqXaXX(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path(final Object v1Id);

    // hasNot functionality

    public abstract Traversal<Vertex, String> get_g_V_whereXnotXoutXcreatedXXX_name();

    // complex and/or functionality

    public abstract Traversal<Vertex,Map<String,String>> get_g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_select_byXnameX();

    // multi-labels

    //public abstract Traversal<Vertex, String> get_g_V_asXaX_outXknowsX_asXbX_whereXasXa__bX_outXcreatedX_hasXname__rippleX_name();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eqXbXX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eqXbXX();
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
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neqXbXX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neqXbXX();
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
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX();
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
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX();
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

    /// where(global)

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
    public void g_VX1X_out_aggregateXxX_out_whereXwithout_xX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_aggregateXxX_out_whereXwithoutXaXX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals("ripple", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_withSideEffectXa_g_VX2XX_VX1X_out_whereXneqXaXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_withSideEffectXa_graph_verticesX2XX_VX1X_out_whereXneqXaXX(convertToVertexId("marko"), convertToVertexId("vadas"));
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
        checkResults(Arrays.asList("vadas", "lop", "ripple"), traversal);
    }

    // complex and/or functionality

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_select_byXnameX() {
        Traversal<Vertex,Map<String,String>> traversal = get_g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_select_byXnameX();
        printTraversalForm(traversal);
        int counter = 0;
        while(traversal.hasNext()) {
            final Map<String,String> map = traversal.next();
            assertEquals(2,map.size());
            assertEquals("marko",map.get("a"));
            assertTrue(map.get("b").equals("josh") || map.get("b").equals("vadas"));
            counter++;
        }
        assertEquals(2,counter);
    }


    // multi-labels
   /* @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXknowsX_asXbX_whereXasXa__bX_outXcreatedX_hasXname__rippleX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_asXaX_outXknowsX_asXbX_whereXasXa__bX_outXcreatedX_hasXname__rippleX_name();
        checkResults(Arrays.asList("josh"), traversal);
    } */


    public static class Traversals extends WhereTest {

        /// where(local)

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eqXbXX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where("a", eq("b"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neqXbXX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where("a", neq("b"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where(as("b").has("name", "marko"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where(as("a").out("knows").as("b"));
        }

        /// where(global)

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").in("created").as("b").where("a", neq("b")).values("name");
        }

        @Override
        public Traversal<Vertex, Object> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").in("created").as("b").where(__.as("b").out("created").has("name", "ripple")).values("age", "name");
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
        public Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_whereXwithoutXaXX(final Object v1Id) {
            return g.V(v1Id).out().aggregate("x").out().where(without("x"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_withSideEffectXa_graph_verticesX2XX_VX1X_out_whereXneqXaXX(final Object v1Id, final Object v2Id) {
            return g.withSideEffect("a", graph.vertices(v2Id).next()).V(v1Id).out().where(neq("a"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path(final Object v1Id) {
            return g.V(v1Id).repeat(__.bothE("created").where(without("e")).aggregate("e").otherV()).emit().path();
        }

        // hasNot functionality

        @Override
        public Traversal<Vertex, String> get_g_V_whereXnotXoutXcreatedXXX_name() {
            return g.V().where(not(__.out("created"))).values("name");
        }

        // complex and/or functionality

        @Override
        public Traversal<Vertex,Map<String,String>> get_g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_select_byXnameX() {
              return g.V().as("a").out().as("b").where(and(as("a").out("knows").as("b"),or(as("b").out("created").has("name","ripple"),as("b").in("knows").count().is(not(eq(0)))))).<String>select().by("name");
        }

        // multi-labels

       /* @Override
        public Traversal<Vertex, String> get_g_V_asXaX_outXknowsX_asXbX_whereXasXa__bX_outXcreatedX_hasXname__rippleX_name() {
            return g.V().as("a").out("knows").as("b").where(as("a", "b").out("created").has("name", "ripple")).values("name");
        }*/
    }
}