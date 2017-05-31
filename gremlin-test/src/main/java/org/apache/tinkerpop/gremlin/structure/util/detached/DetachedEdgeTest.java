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
package org.apache.tinkerpop.gremlin.structure.util.detached;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedEdgeTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldHashAndEqualCorrectly() {
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("test", v);
        final Set<Edge> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            set.add(DetachedFactory.detach(e,true));
            set.add(DetachedFactory.detach(e,false));
            set.add(e);
        }
        assertEquals(1, set.size());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge de = DetachedFactory.detach(e, false);
        assertSame(de, DetachedFactory.detach(de, false));
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldConstructDetachedEdge() {
        g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("year", 2002);
        final DetachedEdge detachedEdge = DetachedFactory.detach(g.E(convertToEdgeId("marko", "knows", "vadas")).next(), true);
        assertEquals(convertToEdgeId("marko", "knows", "vadas"), detachedEdge.id());
        assertEquals("knows", detachedEdge.label());
        assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.OUT).next().getClass());
        assertEquals(convertToVertexId("marko"), detachedEdge.vertices(Direction.OUT).next().id());
        assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());
        assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.IN).next().getClass());
        assertEquals(convertToVertexId("vadas"), detachedEdge.vertices(Direction.IN).next().id());
        assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());

        assertEquals(2, IteratorUtils.count(detachedEdge.properties()));
        assertEquals(1, IteratorUtils.count(detachedEdge.properties("year")));
        assertEquals(0.5d, detachedEdge.properties("weight").next().value());
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldConstructDetachedEdgeAsReference() {
        g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("year", 2002);
        final DetachedEdge detachedEdge = DetachedFactory.detach(g.E(convertToEdgeId("marko", "knows", "vadas")).next(), false);
        assertEquals(convertToEdgeId("marko", "knows", "vadas"), detachedEdge.id());
        assertEquals("knows", detachedEdge.label());
        assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.OUT).next().getClass());
        assertEquals(convertToVertexId("marko"), detachedEdge.vertices(Direction.OUT).next().id());
        assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());
        assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.IN).next().getClass());
        assertEquals(convertToVertexId("vadas"), detachedEdge.vertices(Direction.IN).next().id());
        assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());

        assertEquals(0, IteratorUtils.count(detachedEdge.properties()));
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldEvaluateToEqual() {
        assertTrue(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), true).equals(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), true)));
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldHaveSameHashCode() {
        assertEquals(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), true).hashCode(), DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), true).hashCode());
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldAttachToGraph() {
        final Edge toDetach = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final DetachedEdge detachedEdge = DetachedFactory.detach(toDetach, true);
        final Edge attached = detachedEdge.attach(Attachable.Method.get(graph));

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedEdge);
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    public void shouldAttachToVertex() {
        final Edge toDetach = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final Vertex outV = toDetach.vertices(Direction.OUT).next();
        final DetachedEdge detachedEdge = DetachedFactory.detach(toDetach, true);
        final Edge attached = detachedEdge.attach(Attachable.Method.get(outV));

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedEdge);
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotEvaluateToEqualDifferentId() {
        final Object joshCreatedLopEdgeId = convertToEdgeId("josh", "created", "lop");
        final Vertex vOut = g.V(convertToVertexId("josh")).next();
        final Vertex vIn = g.V(convertToVertexId("lop")).next();
        final Edge e = vOut.addEdge("created", vIn, "weight", 0.4d);
        assertFalse(DetachedFactory.detach(g.E(joshCreatedLopEdgeId).next(), true).equals(DetachedFactory.detach(e, true)));
    }

    @Test
    public void shouldConstructDetachedEdgeFromParts() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("x", "a");
        properties.put("y", "b");

        final DetachedEdge de = new DetachedEdge(10, "bought", properties, 1, "person", 2, "product");

        assertEquals(10, de.id());
        assertEquals("bought", de.label());
        assertEquals("person", de.vertices(Direction.OUT).next().label());
        assertEquals(1, de.vertices(Direction.OUT).next().id());
        assertEquals("product", de.vertices(Direction.IN).next().label());
        assertEquals(2, de.vertices(Direction.IN).next().id());

        assertEquals("a", de.properties("x").next().value());
        assertEquals(1, IteratorUtils.count(de.properties("x")));

        assertEquals("a", de.property("x").value());
        assertEquals("x", de.property("x").key());

        assertEquals("b", de.property("y").value());
        assertEquals("y", de.property("y").key());
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowSetProperty() {
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedFactory.detach(e, false);
        detachedEdge.property("test", "test");
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowRemove() {
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedFactory.detach(e, false);
        detachedEdge.remove();
    }
}
