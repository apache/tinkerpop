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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import static org.apache.tinkerpop.gremlin.structure.P.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class WhereTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neq_bX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX();
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
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_b_neqX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neq_bX();
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

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends WhereTest {

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where("a", eq("b"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neq_bX() {
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
    }
}