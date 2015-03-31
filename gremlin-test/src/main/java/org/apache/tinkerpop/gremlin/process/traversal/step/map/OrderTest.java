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
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Order;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.*;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class OrderTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_name_order();

    public abstract Traversal<Vertex, String> get_g_V_name_order_byXa1_b1X_byXb2_a2X();

    public abstract Traversal<Vertex, String> get_g_V_order_byXname_incrX_name();

    public abstract Traversal<Vertex, String> get_g_V_order_byXnameX_name();

    public abstract Traversal<Vertex, Double> get_g_V_outE_order_byXweight_decrX_weight();

    public abstract Traversal<Vertex, String> get_g_V_order_byXname_a1_b1X_byXname_b2_a2X_name();

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_select();

    public abstract Traversal<Vertex, Map<Integer, Integer>> get_g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalueDecrX_byXkeyIncrX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_order_byXoutE_count__decrX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_name_order() {

        final Traversal<Vertex, String> traversal = get_g_V_name_order();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(names.size(), 6);
        assertEquals("josh", names.get(0));
        assertEquals("lop", names.get(1));
        assertEquals("marko", names.get(2));
        assertEquals("peter", names.get(3));
        assertEquals("ripple", names.get(4));
        assertEquals("vadas", names.get(5));
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_name_order_byXa1_b1X_byXb2_a2X() {
        final Traversal<Vertex, String> traversal = get_g_V_name_order_byXa1_b1X_byXb2_a2X();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(names.size(), 6);
        assertEquals("marko", names.get(0));
        assertEquals("vadas", names.get(1));
        assertEquals("peter", names.get(2));
        assertEquals("ripple", names.get(3));
        assertEquals("josh", names.get(4));
        assertEquals("lop", names.get(5));
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_order_byXname_incrX_name() {
        Arrays.asList(get_g_V_order_byXname_incrX_name(), get_g_V_order_byXnameX_name()).forEach(traversal -> {
            printTraversalForm(traversal);
            final List<String> names = traversal.toList();
            assertEquals(names.size(), 6);
            assertEquals("josh", names.get(0));
            assertEquals("lop", names.get(1));
            assertEquals("marko", names.get(2));
            assertEquals("peter", names.get(3));
            assertEquals("ripple", names.get(4));
            assertEquals("vadas", names.get(5));
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_outE_order_byXweight_decrX_weight() {
        final Traversal<Vertex, Double> traversal = get_g_V_outE_order_byXweight_decrX_weight();
        printTraversalForm(traversal);
        final List<Double> weights = traversal.toList();
        assertEquals(6, weights.size());
        assertEquals(Double.valueOf(1.0d), weights.get(0));
        assertEquals(Double.valueOf(1.0d), weights.get(1));
        assertEquals(Double.valueOf(0.5d), weights.get(2));
        assertEquals(Double.valueOf(0.4d), weights.get(3));
        assertEquals(Double.valueOf(0.4d), weights.get(4));
        assertEquals(Double.valueOf(0.2d), weights.get(5));

    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_order_byXname_a1_b1X_byXname_b2_a2X_name() {
        final Traversal<Vertex, String> traversal = get_g_V_order_byXname_a1_b1X_byXname_b2_a2X_name();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(names.size(), 6);
        assertEquals("marko", names.get(0));
        assertEquals("vadas", names.get(1));
        assertEquals("peter", names.get(2));
        assertEquals("ripple", names.get(3));
        assertEquals("josh", names.get(4));
        assertEquals("lop", names.get(5));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_select() {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_select();
        printTraversalForm(traversal);
        int counter = 0;
        int markoCounter = 0;
        int joshCounter = 0;
        int peterCounter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, Vertex> bindings = traversal.next();
            assertEquals(2, bindings.size());
            if (bindings.get("a").id().equals(convertToVertexId("marko"))) {
                assertEquals(convertToVertexId("lop"), bindings.get("b").id());
                markoCounter++;
            } else if (bindings.get("a").id().equals(convertToVertexId("josh"))) {
                assertTrue((bindings.get("b")).id().equals(convertToVertexId("lop")) || bindings.get("b").id().equals(convertToVertexId("ripple")));
                joshCounter++;
            } else if (bindings.get("a").id().equals(convertToVertexId("peter"))) {
                assertEquals(convertToVertexId("lop"), bindings.get("b").id());
                peterCounter++;
            } else {
                fail("This state should not have been reachable");
            }


        }
        assertEquals(4, markoCounter + joshCounter + peterCounter);
        assertEquals(1, markoCounter);
        assertEquals(1, peterCounter);
        assertEquals(2, joshCounter);
        assertEquals(4, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalueDecrX_byXkeyIncrX() {
        final Traversal<Vertex, Map<Integer, Integer>> traversal = get_g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalueDecrX_byXkeyIncrX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final Map<Integer, Integer> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(4, map.size());
        final Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();
        Map.Entry<Integer, Integer> entry = iterator.next();
        assertEquals(3, entry.getKey().intValue());
        assertEquals(87, entry.getValue().intValue());
        entry = iterator.next();
        assertEquals(2, entry.getKey().intValue());
        assertEquals(58, entry.getValue().intValue());
        entry = iterator.next();
        assertEquals(1, entry.getKey().intValue());
        assertEquals(29, entry.getValue().intValue());
        entry = iterator.next();
        assertEquals(4, entry.getKey().intValue());
        assertEquals(29, entry.getValue().intValue());
        assertFalse(iterator.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_order_byXoutE_count__decrX() {
        Arrays.asList(get_g_V_order_byXoutE_count__decrX()).forEach(traversal -> {
            printTraversalForm(traversal);
            final List<Vertex> vertices = traversal.toList();
            assertEquals(vertices.size(), 6);
            assertEquals("marko", vertices.get(0).value("name"));
            assertEquals("josh", vertices.get(1).value("name"));
            assertEquals("peter", vertices.get(2).value("name"));
            assertTrue(vertices.get(3).value("name").equals("vadas") || vertices.get(3).value("name").equals("ripple") || vertices.get(3).value("name").equals("lop"));
            assertTrue(vertices.get(4).value("name").equals("vadas") || vertices.get(4).value("name").equals("ripple") || vertices.get(4).value("name").equals("lop"));
            assertTrue(vertices.get(5).value("name").equals("vadas") || vertices.get(5).value("name").equals("ripple") || vertices.get(5).value("name").equals("lop"));
        });
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends OrderTest {

        @Override
        public Traversal<Vertex, String> get_g_V_name_order() {
            return g.V().<String>values("name").order();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_order_byXa1_b1X_byXb2_a2X() {
            return g.V().<String>values("name").order().by((a, b) -> a.substring(1, 2).compareTo(b.substring(1, 2))).by((a, b) -> b.substring(2, 3).compareTo(a.substring(2, 3)));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXname_incrX_name() {
            return g.V().order().by("name", Order.incr).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXnameX_name() {
            return g.V().order().by("name", Order.incr).values("name");
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_outE_order_byXweight_decrX_weight() {
            return g.V().outE().order().by("weight", Order.decr).values("weight");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXname_a1_b1X_byXname_b2_a2X_name() {
            return g.V().order().
                    <String>by("name", (a, b) -> a.substring(1, 2).compareTo(b.substring(1, 2))).
                    <String>by("name", (a, b) -> b.substring(2, 3).compareTo(a.substring(2, 3))).values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_select() {
            return g.V().as("a").out("created").as("b").order().by(Order.shuffle).select();
        }

        @Override
        public Traversal<Vertex, Map<Integer, Integer>> get_g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalueDecrX_byXkeyIncrX(final Object v1Id) {
            return g.V(v1Id).map(v -> {
                final Map<Integer, Integer> map = new HashMap<>();
                map.put(1, (int) v.get().value("age"));
                map.put(2, (int) v.get().value("age") * 2);
                map.put(3, (int) v.get().value("age") * 3);
                map.put(4, (int) v.get().value("age"));
                return map;
            }).order(Scope.local).by(Order.valueDecr).by(Order.keyIncr);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_order_byXoutE_count__decrX() {
            return g.V().order().by(outE().count(), Order.decr);
        }

    }
}
