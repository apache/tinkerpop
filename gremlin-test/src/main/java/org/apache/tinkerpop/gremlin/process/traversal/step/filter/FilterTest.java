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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class FilterTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXfalseX();

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXtrueX();

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_filterXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_filterXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX();

    public abstract Traversal<Edge, Edge> get_g_E_filterXfalseX();

    public abstract Traversal<Edge, Edge> get_g_E_filterXtrueX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXfalseX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_filterXfalseX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXtrueX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_filterXtrueX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            vertices.add(traversal.next());
        }
        assertEquals(6, counter);
        assertEquals(6, vertices.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXlang_eq_javaX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_filterXlang_eq_javaX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("ripple") ||
                    vertex.value("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_filterXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_filterXage_gt_30X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_filterXage_gt_30X() {
        final  Traversal<Vertex, Vertex> traversal = get_g_VX1X_filterXage_gt_30X(convertToVertexId("josh"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(Integer.valueOf(32), traversal.next().<Integer>value("age"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_filterXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_filterXage_gt_30X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals(Integer.valueOf(32), traversal.next().<Integer>value("age"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("marko") ||
                    vertex.value("name").equals("peter"));
        }
        assertEquals(counter, 2);
        assertEquals(vertices.size(), 2);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_filterXfalseX() {
        final Traversal<Edge, Edge> traversal = get_g_E_filterXfalseX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_filterXtrueX() {
        final Traversal<Edge, Edge> traversal = get_g_E_filterXtrueX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            edges.add(traversal.next());
        }
        assertEquals(6, counter);
        assertEquals(6, edges.size());
        assertFalse(traversal.hasNext());
    }

    public static class Traversals extends FilterTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            return g.V().filter(v -> false);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            return g.V().filter(v -> true);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            return g.V().filter(v -> v.get().<String>property("lang").orElse("none").equals("java"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_filterXage_gt_30X(final Object v1Id) {
            return g.V(v1Id).filter(v -> v.get().<Integer>property("age").orElse(0) > 30);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_filterXage_gt_30X(final Object v1Id) {
            return g.V(v1Id).out().filter(v -> v.get().<Integer>property("age").orElse(0) > 30);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            return g.V().filter(v -> {
                final String name = v.get().value("name");
                return name.startsWith("m") || name.startsWith("p");
            });
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXfalseX() {
            return g.E().filter(e -> false);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXtrueX() {
            return g.E().filter(e -> true);
        }
    }
}
