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
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class MapTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, String> get_g_VX1X_mapXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Integer> get_g_VX1X_outE_label_mapXlengthX(final Object v1Id);

    public abstract Traversal<Vertex, Integer> get_g_VX1X_out_mapXnameX_mapXlengthX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_withPath_V_asXaX_out_mapXa_nameX();

    public abstract Traversal<Vertex, String> get_g_withPath_V_asXaX_out_out_mapXa_name_it_nameX();

    public abstract Traversal<Vertex, Vertex> get_g_V_mapXselectXaXX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_mapXnameX() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_mapXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals(traversal.next(), "marko");
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE_label_mapXlengthX() {
        final Traversal<Vertex, Integer> traversal = get_g_VX1X_outE_label_mapXlengthX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<Integer> lengths = traversal.toList();
        assertTrue(lengths.contains("created".length()));
        assertTrue(lengths.contains("knows".length()));
        assertEquals(lengths.size(), 3);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_mapXnameX_mapXlengthX() {
        final Traversal<Vertex, Integer> traversal = get_g_VX1X_out_mapXnameX_mapXlengthX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<Integer> lengths = traversal.toList();
        assertTrue(lengths.contains("josh".length()));
        assertTrue(lengths.contains("vadas".length()));
        assertTrue(lengths.contains("lop".length()));
        assertEquals(lengths.size(), 3);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_asXaX_out_mapXa_nameX() {
        int marko = 0;
        int peter = 0;
        int josh = 0;
        int other = 0;

        final Traversal<Vertex, String> traversal = get_g_withPath_V_asXaX_out_mapXa_nameX();
        printTraversalForm(traversal);
        while (traversal.hasNext()) {
            final String name = traversal.next();
            if (name.equals("marko")) marko++;
            else if (name.equals("peter")) peter++;
            else if (name.equals("josh")) josh++;
            else other++;
        }
        assertEquals(marko, 3);
        assertEquals(josh, 2);
        assertEquals(peter, 1);
        assertEquals(other, 0);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_asXaX_out_out_mapXa_name_it_nameX() {
        final Traversal<Vertex, String> traversal = get_g_withPath_V_asXaX_out_out_mapXa_name_it_nameX();
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String doubleName = traversal.next();
            assertTrue("markoripple".equals(doubleName) || "markolop".equals(doubleName));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    /** TINKERPOP-782 */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_mapXselectXaXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_mapXselectXaXX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(6, counter);
    }

    public static class Traversals extends MapTest {
        @Override
        public Traversal<Vertex, String> get_g_VX1X_mapXnameX(final Object v1Id) {
            return g.V(v1Id).<String>map(v -> v.get().value("name"));
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_outE_label_mapXlengthX(final Object v1Id) {
            return g.V(v1Id).outE().label().map(l -> l.get().length());
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_out_mapXnameX_mapXlengthX(final Object v1Id) {
            return g.V(v1Id).out().map(v -> v.get().value("name")).map(n -> n.get().toString().length());
        }

        @Override
        public Traversal<Vertex, String> get_g_withPath_V_asXaX_out_mapXa_nameX() {
            return g.withPath().V().as("a").out().<String>map(v -> v.<Vertex>path("a").value("name"));
        }

        @Override
        public Traversal<Vertex, String> get_g_withPath_V_asXaX_out_out_mapXa_name_it_nameX() {
            return g.withPath().V().as("a").out().out().map(v -> v.<Vertex>path("a").<String>value("name") + v.get().<String>value("name"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_mapXselectXaXX() {
            return g.V().as("a").map(select("a"));
        }
    }
}
