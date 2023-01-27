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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldIteratePropertiesOnDetached() {
        final Vertex v = graph.addVertex("name", "daniel", "favoriteColor", "red", "state", "happy");
        final Vertex detached = DetachedFactory.detach(v, true);
        assertEquals("daniel", detached.properties("name").next().value());
        detached.properties().forEachRemaining(p -> {
            if (p.key().equals("name"))
                assertEquals("daniel", p.value());
            else if (p.key().equals("favoriteColor"))
                assertEquals("red", p.value());
            else if (p.key().equals("state"))
                assertEquals("happy", p.value());
            else
                fail("Should be one of the expected keys");
        });
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldHashAndEqualCorrectly() {
        final Vertex v = graph.addVertex();
        final Set<Vertex> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            set.add(DetachedFactory.detach(v, true));
            set.add(DetachedFactory.detach(v, false));
            set.add(v);
        }
        assertEquals(1, set.size());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = graph.addVertex();
        final DetachedVertex dv = DetachedFactory.detach(v, true);
        assertSame(dv, DetachedFactory.detach(dv, true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldConstructDetachedVertex() {
        final Vertex v = graph.addVertex("test", "123");
        final DetachedVertex detachedVertex = DetachedFactory.detach(v, true);

        assertEquals(v.id(), detachedVertex.id());
        assertEquals(v.label(), detachedVertex.label());
        assertEquals("123", detachedVertex.value("test"));
        assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldConstructDetachedVertexAsReference() {
        final Vertex v = graph.addVertex("test", "123", "test", "321");
        final DetachedVertex detachedVertex = DetachedFactory.detach(v, false);

        assertEquals(v.id(), detachedVertex.id());
        assertEquals(v.label(), detachedVertex.label());
        assertEquals(0, IteratorUtils.count(detachedVertex.properties()));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldDetachVertexWithMultiPropertiesAndMetaProperties() {
        final DetachedVertex v1 = DetachedFactory.detach(convertToVertex(graph, "marko"), true);

        assertEquals("person", v1.label());
        assertEquals(2, v1.keys().size());
        v1.properties("location").forEachRemaining(vp -> {
            assertTrue(vp instanceof DetachedVertexProperty);
            if (vp.value().equals("san diego")) {
                assertEquals(1997, (int) vp.value("startTime"));
                assertEquals(2001, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(vp.properties()));
            } else if (vp.value().equals("santa cruz")) {
                assertEquals(2001, (int) vp.value("startTime"));
                assertEquals(2004, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(vp.properties()));
            } else if (vp.value().equals("brussels")) {
                assertEquals(2004, (int) vp.value("startTime"));
                assertEquals(2005, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(vp.properties()));
            } else if (vp.value().equals("santa fe")) {
                assertEquals(2005, (int) vp.value("startTime"));
                assertEquals(1, (int) IteratorUtils.count(vp.properties()));
            } else {
                fail("Found a value that should be there");
            }
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldEvaluateToEqual() {
        assertTrue(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).equals(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true)));
        assertTrue(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).equals(g.V(convertToVertexId("marko")).next()));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldHaveSameHashCode() {
        assertEquals(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).hashCode(), DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).hashCode());
        assertEquals(DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).hashCode(), g.V(convertToVertexId("marko")).next().hashCode());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToGraph() {
        final Vertex toDetach = g.V(convertToVertexId("josh")).next();
        final DetachedVertex detachedVertex = DetachedFactory.detach(toDetach, true);
        final Vertex attached = detachedVertex.attach(Attachable.Method.get(graph));

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedVertex);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToVertex() {
        final Vertex toDetach = g.V(convertToVertexId("josh")).next();
        final DetachedVertex detachedVertex = DetachedFactory.detach(toDetach, true);
        final Vertex attached = detachedVertex.attach(Attachable.Method.get(toDetach));

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedVertex);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_INTEGER_VALUES)
    public void shouldNotEvaluateToEqualDifferentId() {
        final DetachedVertex originalMarko = DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true);
        final Vertex secondMarko = graph.addVertex("name", "marko", "age", 29);
        assertFalse(DetachedFactory.detach(secondMarko, true).equals(originalMarko));
    }

    @Test
    public void shouldConstructDetachedVertexFromParts() {
        final Map<String, Object> properties = new HashMap<>();
        final Map<String, Object> propX1 = new HashMap<>();
        propX1.put("value", "a");
        propX1.put("id", 123);
        propX1.put("label", "x");
        final Map<String, Object> propX2 = new HashMap<>();
        propX2.put("value", "c");
        propX2.put("id", 124);
        propX2.put("label", "x");
        properties.put("x", Arrays.asList(propX1, propX2));

        final Map<String, Object> propY1 = new HashMap<>();
        propY1.put("value", "b");
        propY1.put("id", 125);
        propY1.put("label", "y");
        final Map<String, Object> propY2 = new HashMap<>();
        propY2.put("value", "d");
        propY2.put("id", 126);
        propY2.put("label", "y");
        properties.put("y", Arrays.asList(propY1, propY2));

        final DetachedVertex dv = new DetachedVertex(1, "test", properties);

        assertEquals(1, dv.id());
        assertEquals("test", dv.label());

        final List<VertexProperty<Object>> propertyX = IteratorUtils.list(dv.properties("x"));
        assertEquals(2, propertyX.size());
        assertTrue(propertyX.stream().allMatch(p ->
                p.label().equals(p.key())
                        && (p.id().equals(123) || p.id().equals(124))
                        && (p.value().equals("a") || p.value().equals("c"))
                        && !p.properties().hasNext()));

    }

    @Test
    public void shouldConstructDetachedVertexFromPartsWithPropertiesOnProperties() {
        final Map<String, Object> properties = new HashMap<>();
        final Map<String, Object> propX1 = new HashMap<>();
        propX1.put("value", "a");
        propX1.put("id", 123);
        propX1.put("label", "x");
        propX1.put("properties", ElementHelper.asMap("propX1a", "a", "propX11", 1, "same", 123.01d, "extra", "something"));
        final Map<String, Object> propX2 = new HashMap<>();
        propX2.put("value", "c");
        propX2.put("id", 124);
        propX2.put("label", "x");
        properties.put("x", Arrays.asList(propX1, propX2));

        final Map<String, Object> propY1 = new HashMap<>();
        propY1.put("value", "b");
        propY1.put("id", 125);
        propY1.put("label", "");
        final Map<String, Object> propY2 = new HashMap<>();
        propY2.put("value", "d");
        propY2.put("id", 126);
        propY2.put("label", "y");
        properties.put("y", Arrays.asList(propY1, propY2));

        final DetachedVertex dv = new DetachedVertex(1, "test", properties);

        assertEquals(1, dv.id());
        assertEquals("test", dv.label());

        final List<VertexProperty<Object>> propertyX = IteratorUtils.list(dv.properties("x"));
        assertEquals(2, propertyX.size());
        assertTrue(propertyX.stream().allMatch(p ->
                p.label().equals(p.key())
                        && (p.id().equals(123) || p.id().equals(124))
                        && (p.value().equals("a") || p.value().equals("c"))));

        // there should be only one with properties on properties
        final VertexProperty<?> propertyOnProperty = propertyX.stream().filter(p -> p.properties().hasNext()).findAny().get();
        assertEquals("a", propertyOnProperty.properties("propX1a").next().value());
        assertEquals(1, propertyOnProperty.properties("propX11").next().value());
        assertEquals(123.01d, propertyOnProperty.properties("same").next().value());
        assertEquals("something", propertyOnProperty.properties("extra").next().value());
        assertEquals(4, IteratorUtils.count(propertyOnProperty.properties()));
    }


    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowAddEdge() {
        final Vertex v = graph.addVertex();
        final DetachedVertex detachedVertex = DetachedFactory.detach(v, true);
        detachedVertex.addEdge("test", null);
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowSetProperty() {
        final Vertex v = graph.addVertex();
        final DetachedVertex detachedVertex = DetachedFactory.detach(v, true);
        detachedVertex.property(VertexProperty.Cardinality.single, "test", "test");
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowRemove() {
        final Vertex v = graph.addVertex();
        final DetachedVertex detachedVertex = DetachedFactory.detach(v, true);
        detachedVertex.remove();
    }

    @Test(expected = IllegalStateException.class)
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldNotBeAbleToCallPropertyIfThereAreMultipleProperties() {
        DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true).property("location");
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldBeAbleToCallPropertyIfThereIsASingleProperty() {
        final DetachedVertex dv = DetachedFactory.detach(g.V(convertToVertexId("marko")).next(), true);
        assertEquals("marko", dv.property("name").value());
        assertEquals(29, dv.property("age").value());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldCreateVertex() {
        final DetachedVertex detachedVertex = new DetachedVertex(23, "dog", Collections.emptyMap());
        detachedVertex.attach(Attachable.Method.create(graph));
        assertEquals(7, IteratorUtils.count(graph.vertices()));
        final AtomicInteger dogTimes = new AtomicInteger(0);
        graph.vertices().forEachRemaining(vertex -> {
            if (vertex.label().equals("dog")) {
                dogTimes.incrementAndGet();
            }
        });
        assertEquals(1, dogTimes.get());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldDetachCollections() {
        // single element
        final Vertex vertex = DetachedFactory.detach(g.V().has("name", "marko").next(), true);
        assertTrue(vertex instanceof DetachedVertex);
        // list nest
        final List<Vertex> vertices = DetachedFactory.detach(g.V().hasLabel("software").fold().next(), true);
        for (final Vertex v : vertices) {
            assertTrue(v instanceof DetachedVertex);
            assertEquals("java", v.value("lang"));
        }
        // double nested list
        final List<List<Vertex>> lists = DetachedFactory.detach(g.V().hasLabel("software").fold().fold().next(), true);
        for (final Vertex v : lists.get(0)) {
            assertTrue(v instanceof DetachedVertex);
            assertEquals("java", v.value("lang"));
        }
        // double nested list to set
        final Set<List<Vertex>> set = DetachedFactory.detach(g.V().hasLabel("software").fold().toSet(), true);
        for (final Vertex v : set.iterator().next()) {
            assertTrue(v instanceof DetachedVertex);
            assertEquals("java", v.value("lang"));
        }
        // map keys and values
        Map<Vertex, List<Edge>> map = DetachedFactory.detach(g.V().hasLabel("software").group().by().by(inE().fold()).next(), true);
        for (final Map.Entry<Vertex, List<Edge>> entry : map.entrySet()) {
            assertTrue(entry.getKey() instanceof DetachedVertex);
            assertEquals("java", entry.getKey().value("lang"));
            for (final Edge edge : entry.getValue()) {
                assertTrue(edge instanceof DetachedEdge);
                assertTrue(edge.property("weight").isPresent());
            }
        }
        // map keys and values as sideEffect
        map = DetachedFactory.detach(g.V().hasLabel("software").group("m").by().by(inE().fold()).identity().cap("m").next(), true);
        for (final Map.Entry<Vertex, List<Edge>> entry : map.entrySet()) {
            assertTrue(entry.getKey() instanceof DetachedVertex);
            assertEquals("java", entry.getKey().value("lang"));
            for (final Edge edge : entry.getValue()) {
                assertTrue(edge instanceof DetachedEdge);
                assertTrue(edge.property("weight").isPresent());
            }
        }
    }
}
