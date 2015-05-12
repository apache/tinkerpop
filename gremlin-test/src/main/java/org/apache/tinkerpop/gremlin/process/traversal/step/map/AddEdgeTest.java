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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class AddEdgeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_V_addOutEXexistsWith__g_V__time_nowX();

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("createdBy", edge.label());
            assertEquals(0, IteratorUtils.count(edge.properties()));
            count++;

        }
        assertEquals(1, count);
        assertEquals(7, IteratorUtils.count(graph.edges()));
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("createdBy", edge.label());
            assertEquals(2, edge.<Integer>value("weight").intValue());
            assertEquals(1, IteratorUtils.count(edge.properties()));
            count++;


        }
        assertEquals(1, count);
        assertEquals(7, IteratorUtils.count(graph.edges()));
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_V_addOutEXexistsWith__g_V__time_nowX() {
        final Traversal<Vertex, Edge> traversal = get_g_V_addOutEXexistsWith__g_V__time_nowX();
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("existsWith", edge.label());
            assertEquals("now", edge.value("time"));
            assertEquals(1, IteratorUtils.count(edge.properties()));
            count++;
        }
        assertEquals(36, count);
        assertEquals(42, IteratorUtils.count(graph.edges()));
        for (final Vertex vertex : IteratorUtils.list(graph.vertices())) {
            assertEquals(6, IteratorUtils.count(vertex.edges(Direction.OUT, "existsWith")));
            assertEquals(6, IteratorUtils.count(vertex.edges(Direction.IN, "existsWith")));
        }
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    public static class Traversals extends AddEdgeTest {

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").addOutE("createdBy", "a");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").addOutE("createdBy", "a", "weight", 2);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_addOutEXexistsWith__g_V__time_nowX() {
            return g.V().addOutE("existsWith", g.V(), "time", "now");
        }
    }
}
