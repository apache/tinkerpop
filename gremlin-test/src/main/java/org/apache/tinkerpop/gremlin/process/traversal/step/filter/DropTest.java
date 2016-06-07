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

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class DropTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_drop();

    public abstract Traversal<Vertex, Edge> get_g_V_outE_drop();

    public abstract Traversal<Vertex, VertexProperty> get_g_V_properties_drop();

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
    public void g_V_drop() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_drop();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        assertEquals(0, IteratorUtils.count(g.V()));
        assertEquals(0, IteratorUtils.count(g.E()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_REMOVE_EDGES)
    public void g_V_outE_drop() {
        final Traversal<Vertex, Edge> traversal = get_g_V_outE_drop();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        assertEquals(6, IteratorUtils.count(g.V()));
        assertEquals(0, IteratorUtils.count(g.E()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
    public void g_V_properties_drop() {
        final Traversal<Vertex, VertexProperty> traversal = get_g_V_properties_drop();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        assertEquals(6, IteratorUtils.count(g.V()));
        assertEquals(6, IteratorUtils.count(g.E()));
        g.V().forEachRemaining(vertex -> assertEquals(0, IteratorUtils.count(vertex.properties())));
    }


    public static class Traversals extends DropTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_drop() {
            return g.V().drop();
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_outE_drop() {
            return g.V().outE().drop();
        }

        @Override
        public Traversal<Vertex, VertexProperty> get_g_V_properties_drop() {
            return (Traversal) g.V().properties().drop();
        }
    }
}