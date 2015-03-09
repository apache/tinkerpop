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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AddEdgeTest extends AbstractGremlinTest {
    public abstract Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_VX1X_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<Vertex> cocreators = new ArrayList<>();
        final List<Object> ids = new ArrayList<>();
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            cocreators.add(vertex);
            ids.add(vertex.id());
        }
        assertEquals(cocreators.size(), 3);
        assertTrue(ids.contains(convertToVertexId("marko")));
        assertTrue(ids.contains(convertToVertexId("peter")));
        assertTrue(ids.contains(convertToVertexId("josh")));

        for (Vertex vertex : cocreators) {
            if (vertex.id().equals(convertToVertexId("marko"))) {
                assertEquals(4, IteratorUtils.count(vertex.edges(Direction.OUT, "cocreator")));
                assertEquals(4, IteratorUtils.count(vertex.edges(Direction.IN, "cocreator")));
            } else {
                assertEquals(1, IteratorUtils.count(vertex.edges(Direction.OUT, "cocreator")));
                assertEquals(1, IteratorUtils.count(vertex.edges(Direction.IN, "cocreator")));
            }
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            assertEquals(convertToVertexId("lop"), vertex.id());
            assertEquals(1, IteratorUtils.count(vertex.vertices(Direction.OUT, "createdBy")));
            assertEquals(convertToVertexId("marko"), vertex.vertices(Direction.OUT, "createdBy").next().id());
            assertFalse(vertex.edges(Direction.OUT, "createdBy").next().properties().hasNext());
            count++;

        }
        assertEquals(1, count);
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            assertEquals(convertToVertexId("lop"), vertex.id());
            assertEquals(1, IteratorUtils.count(vertex.vertices(Direction.OUT, "createdBy")));
            assertEquals(convertToVertexId("marko"), vertex.vertices(Direction.OUT, "createdBy").next().id());
            assertEquals(2, vertex.edges(Direction.OUT, "createdBy").next().values("weight").next());
            assertEquals(1, IteratorUtils.count(vertex.edges(Direction.OUT, "createdBy").next().properties()));
            count++;


        }
        assertEquals(1, count);
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class Traversals extends AddEdgeTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").in("created").addBothE("cocreator", "a");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").addOutE("createdBy", "a");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").addOutE("createdBy", "a", "weight", 2);
        }
    }
}
