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

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.SINK;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class VertexTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_VXlistXv1_v2_v3XX_name(final Vertex v1, final Vertex v2, final Vertex v3);

    public abstract Traversal<Vertex, String> get_g_VXlistX1_2_3XX_name(final Object v1Id, final Object v2Id, final Object v3Id);

    public abstract Traversal<Vertex, Vertex> get_g_V();

    public abstract Traversal<Vertex, Vertex> get_g_VXv1X_out(final Vertex v1);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX2X_in(final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX4X_both(final Object v4Id);

    public abstract Traversal<Edge, Edge> get_g_E();

    public abstract Traversal<Edge, Edge> get_g_EXe11X(final Edge e11);

    public abstract Traversal<Edge, Edge> get_g_EX11X(final Object e11Id);

    public abstract Traversal<Edge, Edge> get_g_EXlistXe7_e11XX(final Edge e7, final Edge e11);

    public abstract Traversal<Edge, Edge> get_g_EXe7_e11X(final Edge e7, final Edge e11);

    public abstract Traversal<Vertex, Edge> get_g_VX1X_outE(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_VX2X_inE(final Object v2Id);

    public abstract Traversal<Vertex, Edge> get_g_VX4X_bothE(final Object v4Id);

    public abstract Traversal<Vertex, Edge> get_g_VX4X_bothEXcreatedX(final Object v4Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outE_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX2X_inE_outV(final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV();

    public abstract Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name();

    public abstract Traversal<Vertex, String> get_g_VX1X_outEXknowsX_bothV_name(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXknows_createdX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outEXknowsX_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outEXknows_createdX_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_out_out();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_out_out(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_out_name(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outE_otherV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX4X_bothE_otherV(final Object v4Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX4X_bothE_hasXweight_lt_1X_otherV(final Object v4Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_to_XOUT_knowsX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1_2_3_4X_name(final Object v1Id, final Object v2Id, final Object v3Id, final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name();

    public abstract Traversal<Vertex, Edge> get_g_V_hasLabelXloopsX_bothEXselfX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasLabelXloopsX_bothXselfX();

    // GRAPH VERTEX/EDGE

    @Test
    @LoadGraphWith(MODERN)
    public void g_VXlistX1_2_3XX_name() {
        final Traversal<Vertex, String> traversal = get_g_VXlistX1_2_3XX_name(convertToVertexId(graph, "marko"), convertToVertexId(graph, "vadas"), convertToVertexId(graph, "lop"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "vadas", "lop"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VXlistXv1_v2_v3XX_name() {
        final Traversal<Vertex, String> traversal = get_g_VXlistXv1_v2_v3XX_name(convertToVertex(graph, "marko"), convertToVertex(graph, "vadas"), convertToVertex(graph, "lop"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "vadas", "lop"), traversal);
    }

    // VERTEX ADJACENCY

    @Test
    @LoadGraphWith(MODERN)
    public void g_V() {
        final Traversal<Vertex, Vertex> traversal = get_g_V();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            vertices.add(traversal.next());
        }
        assertEquals(6, vertices.size());
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out(convertToVertexId("marko"));
        assert_g_v1_out(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VXv1X_out() {
        final Traversal<Vertex, Vertex> traversal = get_g_VXv1X_out(convertToVertex(graph, "marko"));
        assert_g_v1_out(traversal);
    }

    private void assert_g_v1_out(final Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("vadas") ||
                    vertex.value("name").equals("josh") ||
                    vertex.value("name").equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_in() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX2X_in(convertToVertexId("vadas"));
        assert_g_v2_in(traversal);
    }

    private void assert_g_v2_in(final Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals(traversal.next().value("name"), "marko");
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_both() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX4X_both(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("marko") ||
                    vertex.value("name").equals("ripple") ||
                    vertex.value("name").equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, vertices.size());
    }

    // EDGE ADJACENCY

    @Test
    @LoadGraphWith(MODERN)
    public void g_E() {
        final Traversal<Edge, Edge> traversal = get_g_E();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            edges.add(traversal.next());
        }
        assertEquals(6, edges.size());
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_EX11X() {
        final Object edgeId = convertToEdgeId("josh", "created", "lop");
        final Traversal<Edge, Edge> traversal = get_g_EX11X(edgeId);
        assert_g_EX11X(edgeId, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_EXe11X() {
        final Edge edge = convertToEdge(graph, "josh", "created", "lop");
        final Traversal<Edge, Edge> traversal = get_g_EXe11X(edge);
        assert_g_EX11X(edge.id(), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_EXe7_e11X() {
        final Edge e7 = convertToEdge(graph, "marko", "knows", "vadas");
        final Edge e11 = convertToEdge(graph, "josh", "created", "lop");
        final Traversal<Edge, Edge> traversal = get_g_EXe7_e11X(e7,e11);

        printTraversalForm(traversal);
        final List<Edge> list = traversal.toList();
        assertEquals(2, list.size());
        assertThat(list, containsInAnyOrder(e7, e11));
    }

    @Test
    @Ignore("g.E(Arrays.asList(e7, e11)) is not valid for gremlin-lang")
    @LoadGraphWith(MODERN)
    public void g_EXlistXe7_e11XX() {
        final Edge e7 = convertToEdge(graph, "marko", "knows", "vadas");
        final Edge e11 = convertToEdge(graph, "josh", "created", "lop");
        final Traversal<Edge, Edge> traversal = get_g_EXlistXe7_e11XX(e7,e11);

        printTraversalForm(traversal);
        final List<Edge> list = traversal.toList();
        assertEquals(2, list.size());
        assertThat(list, containsInAnyOrder(e7, e11));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_EX11AsStringX() {
        final Object edgeId = convertToEdgeId("josh", "created", "lop");
        final Traversal<Edge, Edge> traversal = get_g_EX11X(edgeId.toString());
        assert_g_EX11X(edgeId, traversal);
    }

    private void assert_g_EX11X(final Object edgeId, final Traversal<Edge, Edge> traversal) {
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Edge e = traversal.next();
        assertEquals(edgeId, e.id());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_outE(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Edge edge = traversal.next();
            edges.add(edge);
            assertTrue(edge.label().equals("knows") || edge.label().equals("created"));
        }
        assertEquals(3, counter);
        assertEquals(3, edges.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_inE() {
        final Traversal<Vertex, Edge> traversal = get_g_VX2X_inE(convertToVertexId("vadas"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals(traversal.next().label(), "knows");
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_bothEXcreatedX() {
        final Traversal<Vertex, Edge> traversal = get_g_VX4X_bothEXcreatedX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Edge edge = traversal.next();
            edges.add(edge);
            assertTrue(edge.label().equals("created"));
            assertEquals(edge.outVertex().id(), convertToVertexId("josh"));
        }
        assertEquals(2, counter);
        assertEquals(2, edges.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_bothE() {
        final Traversal<Vertex, Edge> traversal = get_g_VX4X_bothE(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Edge edge = traversal.next();
            edges.add(edge);
            assertTrue(edge.label().equals("knows") || edge.label().equals("created"));
        }
        assertEquals(3, counter);
        assertEquals(3, edges.size());
    }

    // EDGE/VERTEX ADJACENCY

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outE_inV(convertToVertexId("marko"));
        this.assert_g_v1_out(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_inE_outV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX2X_inE_outV(convertToVertexId("vadas"));
        this.assert_g_v2_in(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_hasXweight_1X_outV() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_outE_hasXweight_1X_outV();
        printTraversalForm(traversal);
        int counter = 0;
        final Map<Object, Integer> counts = new HashMap<>();
        while (traversal.hasNext()) {
            final Object id = traversal.next().id();
            final int previousCount = counts.getOrDefault(id, 0);
            counts.put(id, previousCount + 1);
            counter++;
        }
        assertEquals(2, counts.size());
        assertEquals(1, counts.get(convertToVertexId("marko")).intValue());
        assertEquals(1, counts.get(convertToVertexId("josh")).intValue());

        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_outE_inV_inE_inV_both_name() {
        final Traversal<Vertex, String> traversal = get_g_V_out_outE_inV_inE_inV_both_name();
        printTraversalForm(traversal);
        int counter = 0;
        final Map<String, Integer> counts = new HashMap<>();
        while (traversal.hasNext()) {
            final String key = traversal.next();
            final int previousCount = counts.getOrDefault(key, 0);
            counts.put(key, previousCount + 1);
            counter++;
        }
        assertEquals(3, counts.size());
        assertEquals(4, counts.get("josh").intValue());
        assertEquals(3, counts.get("marko").intValue());
        assertEquals(3, counts.get("peter").intValue());

        assertEquals(10, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outEXknowsX_bothV_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_outEXknowsX_bothV_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(4, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("vadas"));
        names.remove("marko");
        assertEquals(3, names.size());
        names.remove("marko");
        assertEquals(2, names.size());
        names.remove("josh");
        assertEquals(1, names.size());
        names.remove("vadas");
        assertEquals(0, names.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE_otherV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outE_otherV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("vadas") ||
                    vertex.value("name").equals("josh") ||
                    vertex.value("name").equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_bothE_otherV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX4X_bothE_otherV(convertToVertexId("josh"));
        printTraversalForm(traversal);
        final List<Vertex> vertices = traversal.toList();
        assertEquals(3, vertices.size());
        assertTrue(vertices.stream().anyMatch(v -> v.value("name").equals("marko")));
        assertTrue(vertices.stream().anyMatch(v -> v.value("name").equals("ripple")));
        assertTrue(vertices.stream().anyMatch(v -> v.value("name").equals("lop")));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_bothE_hasXweight_lt_1X_otherV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX4X_bothE_hasXweight_lt_1X_otherV(convertToVertexId("josh"));
        printTraversalForm(traversal);
        final List<Vertex> vertices = traversal.toList();
        assertEquals(1, vertices.size());
        assertEquals(vertices.get(0).value("name"), "lop");
        assertFalse(traversal.hasNext());
    }

    // VERTEX EDGE LABEL ADJACENCY

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXknowsX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX(convertToVertexId("marko"));
        assert_g_v1_outXknowsX(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1AsStringX_outXknowsX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX(convertToVertexId("marko").toString());
        assert_g_v1_outXknowsX(traversal);
    }

    private void assert_g_v1_outXknowsX(final Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("vadas") ||
                    vertex.value("name").equals("josh"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXknows_createdX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknows_createdX(convertToVertexId("marko"));
        this.assert_g_v1_out(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outEXknowsX_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outEXknowsX_inV(convertToVertexId("marko"));
        this.assert_g_v1_outXknowsX(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outEXknows_createdX_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outEXknows_createdX_inV(convertToVertexId("marko"));
        this.assert_g_v1_out(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_out_out();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("lop") ||
                    vertex.value("name").equals("ripple"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_out_out() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_out_out(convertToVertexId("marko"));
        assertFalse(traversal.hasNext());
    }

    // PROPERTY TESTING

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_out_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next();
            names.add(name);
            assertTrue(name.equals("vadas") ||
                    name.equals("josh") ||
                    name.equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, names.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_to_XOUT_knowsX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_to_XOUT_knowsX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            final String name = vertex.value("name");
            assertTrue(name.equals("vadas") || name.equals("josh"));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1_2_3_4X_name() {
        final Object vLop = convertToVertexId("lop");
        g.V(vLop).drop().iterate();
        final Traversal<Vertex, String> traversal = get_g_VX1_2_3_4X_name(convertToVertexId("marko"), convertToVertexId("vadas"), vLop, convertToVertexId("josh"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "vadas", "josh"), traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("lop", "lop", "lop", "lop", "ripple", "ripple", "ripple", "ripple"), traversal);
    }

    @Test
    @LoadGraphWith(SINK)
    public void g_V_hasLabelXloopsX_bothEXselfX() {
        final Traversal<Vertex, Edge> traversal = get_g_V_hasLabelXloopsX_bothEXselfX();
        printTraversalForm(traversal);

        List<Edge> edges = traversal.toList();
        assertEquals(2, edges.size());
        assertEquals(edges.get(0), edges.get(1));
    }

    @Test
    @LoadGraphWith(SINK)
    public void g_V_hasLabelXloopsX_bothXselfX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasLabelXloopsX_bothXselfX();
        printTraversalForm(traversal);

        List<Vertex> vertices = traversal.toList();
        assertEquals(2, vertices.size());
        assertEquals(vertices.get(0), vertices.get(1));
    }

    public static class Traversals extends VertexTest {

        @Override
        public Traversal<Vertex, String> get_g_VXlistXv1_v2_v3XX_name(final Vertex v1, final Vertex v2, final Vertex v3) {
            return g.V(Arrays.asList(v1, v2, v3)).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VXlistX1_2_3XX_name(final Object v1Id, final Object v2Id, final Object v3Id) {
            return g.V(Arrays.asList(v1Id, v2Id, v3Id)).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V() {
            return g.V();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VXv1X_out(final Vertex v1) {
            return g.V(v1).out();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out(final Object v1Id) {
            return g.V(v1Id).out();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_in(final Object v2Id) {
            return g.V(v2Id).in();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_both(final Object v4Id) {
            return g.V(v4Id).both();
        }

        @Override
        public Traversal<Edge, Edge> get_g_E() {
            return g.E();
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE(final Object v1Id) {
            return g.V(v1Id).outE();
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX2X_inE(final Object v2Id) {
            return g.V(v2Id).inE();
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_bothE(final Object v4Id) {
            return g.V(v4Id).bothE();
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_bothEXcreatedX(final Object v4Id) {
            return g.V(v4Id).bothE("created");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_inV(final Object v1Id) {
            return g.V(v1Id).outE().inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_inE_outV(final Object v2Id) {
            return g.V(v2Id).inE().outV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV() {
            return g.V().outE().has("weight", 1.0d).outV();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name() {
            return g.V().out().outE().inV().inE().inV().both().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_outEXknowsX_bothV_name(final Object v1Id) {
            return g.V(v1Id).outE("knows").bothV().values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX(final Object v1Id) {
            return g.V(v1Id).out("knows");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknows_createdX(final Object v1Id) {
            return g.V(v1Id).out("knows", "created");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outEXknowsX_inV(final Object v1Id) {
            return g.V(v1Id).outE("knows").inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outEXknows_createdX_inV(final Object v1Id) {
            return g.V(v1Id).outE("knows", "created").inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_otherV(final Object v1Id) {
            return g.V(v1Id).outE().otherV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_bothE_otherV(final Object v4Id) {
            return g.V(v4Id).bothE().otherV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_bothE_hasXweight_lt_1X_otherV(Object v4Id) {
            return g.V(v4Id).bothE().has("weight", P.lt(1d)).otherV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out() {
            return g.V().out().out();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_out_out(final Object v1Id) {
            return g.V(v1Id).out().out().out();
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_name(final Object v1Id) {
            return g.V(v1Id).out().values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_to_XOUT_knowsX(final Object v1Id) {
            return g.V(v1Id).to(Direction.OUT, "knows");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1_2_3_4X_name(final Object v1Id, final Object v2Id, final Object v3Id, final Object v4Id) {
            return g.V(v1Id, v2Id, v3Id, v4Id).values("name");
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX11X(final Object e11Id) {
            return g.E(e11Id);
        }

        @Override
        public Traversal<Edge, Edge> get_g_EXe11X(final Edge e11) {
            return g.E(e11);
        }

        @Override
        public Traversal<Edge, Edge> get_g_EXlistXe7_e11XX(final Edge e7, final Edge e11) {
            return g.E(Arrays.asList(e7, e11));
        }

        @Override
        public Traversal<Edge, Edge> get_g_EXe7_e11X(final Edge e7, final Edge e11) {
            return g.E(e7, e11);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name() {
            return g.V().hasLabel("person").V().hasLabel("software").values("name");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_hasLabelXloopsX_bothEXselfX() {
            return g.V().hasLabel("loops").bothE("self");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXloopsX_bothXselfX() {
            return g.V().hasLabel("loops").both("self");
        }
    }
}
