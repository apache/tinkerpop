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
package org.apache.tinkerpop.gremlin.structure.util.reference;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceVertexTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldHashAndEqualCorrectly() {
        final Vertex v = graph.addVertex();
        final Set<Vertex> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            set.add(ReferenceFactory.detach(v));
            set.add(v);
        }
        assertEquals(1, set.size());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = graph.addVertex();
        final ReferenceVertex referenceVertex = ReferenceFactory.detach(v);
        assertSame(referenceVertex, ReferenceFactory.detach(referenceVertex));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldConstructReferenceVertex() {
        final Vertex v = graph.addVertex("test", "123");
        final ReferenceVertex referenceVertex = ReferenceFactory.detach(v);

        assertEquals(v.id(), referenceVertex.id());
        assertEquals(0, IteratorUtils.count(referenceVertex.properties()));
    }


    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldEvaluateToEqualForVerticesAndDetachments() {
        assertTrue(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next()).equals(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next())));
        assertTrue(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next()).equals(g.V(convertToVertexId("marko")).next()));
        assertTrue(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next()).equals(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true)));
        assertTrue(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next()).equals(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), false)));
        // reverse
        assertTrue(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next().equals(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next()))));
        assertTrue(g.V(convertToVertexId("marko")).next().equals(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next())));
        assertTrue(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).equals(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next())));
        assertTrue(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), false).equals(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next())));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldHaveSameHashCode() {
        assertEquals(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next()).hashCode(), ReferenceFactory.detach(g.V(convertToVertexId("marko")).next()).hashCode());
        assertEquals(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next()).hashCode(), g.V(convertToVertexId("marko")).next().hashCode());
        assertEquals(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next()).hashCode(), DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), false).hashCode());
        assertEquals(ReferenceFactory.detach(g.V(convertToVertexId("marko")).next()).hashCode(), DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).hashCode());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToGraph() {
        final Vertex v = g.V(convertToVertexId("josh")).next();
        final ReferenceVertex referenceVertex = ReferenceFactory.detach(v);
        final Vertex attachedV = referenceVertex.attach(Attachable.Method.get(graph));

        assertEquals(v, attachedV);
        assertFalse(attachedV instanceof ReferenceVertex);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToVertex() {
        final Vertex v = g.V(convertToVertexId("josh")).next();
        final ReferenceVertex referenceVertex = ReferenceFactory.detach(v);
        final Vertex attachedV = referenceVertex.attach(Attachable.Method.get(v));

        assertEquals(v, attachedV);
        assertFalse(attachedV instanceof ReferenceVertex);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_INTEGER_VALUES)
    public void shouldNotEvaluateToEqualDifferentId() {
        final ReferenceVertex originalMarko = ReferenceFactory.detach(g.V(convertToVertexId("marko")).next());
        final Vertex secondMarko = graph.addVertex("name", "marko", "age", 29);
        assertFalse(ReferenceFactory.detach(secondMarko).equals(originalMarko));
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowAddEdge() {
        final Vertex v = graph.addVertex();
        final ReferenceVertex rv = ReferenceFactory.detach(v);
        rv.addEdge("test", null);
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowSetProperty() {
        final Vertex v = graph.addVertex();
        final ReferenceVertex rv = ReferenceFactory.detach(v);
        rv.property(VertexProperty.Cardinality.single, "test", "test");
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowRemove() {
        final Vertex v = graph.addVertex();
        final ReferenceVertex rv = ReferenceFactory.detach(v);
        rv.remove();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotHaveAnyProperties() {
        final ReferenceVertex rv = ReferenceFactory.detach(g.V(convertToVertexId("marko")).next());
        assertFalse(rv.properties().hasNext());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotHaveAnyEdges() {
        final ReferenceVertex rv = ReferenceFactory.detach(g.V(convertToVertexId("marko")).next());
        assertFalse(rv.edges(Direction.BOTH).hasNext());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotHaveAnyVertices() {
        final ReferenceVertex rv = ReferenceFactory.detach(g.V(convertToVertexId("marko")).next());
        assertFalse(rv.vertices(Direction.BOTH).hasNext());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldDetachCollections() {
        // single element
        final Vertex vertex = ReferenceFactory.detach(g.V().has("name", "marko").next());
        assertTrue(vertex instanceof ReferenceVertex);
        // list nest
        final List<Vertex> vertices = ReferenceFactory.detach(g.V().hasLabel("software").fold().next());
        for (final Vertex v : vertices) {
            assertTrue(v instanceof ReferenceVertex);
            assertEquals("java", v.value("lang"));
        }
        // double nested list
        final List<List<Vertex>> lists = ReferenceFactory.detach(g.V().hasLabel("software").fold().fold().next());
        for (final Vertex v : lists.get(0)) {
            assertTrue(v instanceof ReferenceVertex);
            assertEquals("java", v.value("lang"));
        }
        // double nested list to set
        final Set<List<Vertex>> set = ReferenceFactory.detach(g.V().hasLabel("software").fold().toSet());
        for (final Vertex v : set.iterator().next()) {
            assertTrue(v instanceof ReferenceVertex);
            assertEquals("java", v.value("lang"));
        }
        // map keys and values
        Map<Vertex, List<Edge>> map = ReferenceFactory.detach(g.V().hasLabel("software").group().by().by(inE().fold()).next());
        for (final Map.Entry<Vertex, List<Edge>> entry : map.entrySet()) {
            assertTrue(entry.getKey() instanceof ReferenceVertex);
            assertEquals("java", entry.getKey().value("lang"));
            for (final Edge edge : entry.getValue()) {
                assertTrue(edge instanceof ReferenceEdge);
                assertTrue(edge.property("weight").isPresent());
            }
        }
        // map keys and values as sideEffect
        map = ReferenceFactory.detach(g.V().hasLabel("software").group("m").by().by(inE().fold()).identity().cap("m").next());
        for (final Map.Entry<Vertex, List<Edge>> entry : map.entrySet()) {
            assertTrue(entry.getKey() instanceof ReferenceVertex);
            assertEquals("java", entry.getKey().value("lang"));
            for (final Edge edge : entry.getValue()) {
                assertTrue(edge instanceof ReferenceEdge);
                assertTrue(edge.property("weight").isPresent());
            }
        }
    }
}
