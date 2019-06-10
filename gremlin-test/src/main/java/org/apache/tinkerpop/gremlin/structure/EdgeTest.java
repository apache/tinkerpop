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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.ExceptionCoverage;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_BOOLEAN_VALUES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_DOUBLE_VALUES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_FLOAT_VALUES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_INTEGER_VALUES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_LONG_VALUES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_STRING_VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Enclosed.class)
public class EdgeTest {

    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "labelCanNotBeNull",
            "labelCanNotBeEmpty",
            "labelCanNotBeAHiddenKey"
    })
    public static class BasicEdgeTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        public void shouldValidateEquality() {
            final Vertex v = graph.addVertex();
            final Edge e1 = v.addEdge("self", v);
            final Edge e2 = v.addEdge("self", v);

            assertEquals(e1, e1);
            assertEquals(e2, e2);
            assertNotEquals(e1, e2);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        public void shouldValidateIdEquality() {
            final Vertex v = graph.addVertex();
            final Edge e1 = v.addEdge("self", v);
            final Edge e2 = v.addEdge("self", v);

            assertEquals(e1.id(), e1.id());
            assertEquals(e2.id(), e2.id());
            assertEquals(e1.id().toString(), e1.id().toString());
            assertEquals(e2.id().toString(), e2.id().toString());
            assertNotEquals(e1.id(), e2.id());
            assertNotEquals(e1.id().toString(), e2.id().toString());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveStandardStringRepresentation() {
            final Vertex v1 = graph.addVertex();
            final Vertex v2 = graph.addVertex();
            final Edge e = v1.addEdge("friends", v2);

            assertEquals(StringFactory.edgeString(e), e.toString());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingNullEdgeLabel() {
            final Vertex v = graph.addVertex();
            try {
                v.addEdge(null, v);
                fail("Call to Vertex.addEdge() should throw an exception when label is null");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeNull(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingNullVertex() {
            final Vertex v = graph.addVertex();
            try {
                v.addEdge("tonothing", null);
                fail("Call to Vertex.addEdge() should throw an exception when vertex is null");
            } catch (Exception ex) {
                validateException(Graph.Exceptions.argumentCanNotBeNull("vertex"), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingEmptyVertexLabel() {
            final Vertex v = graph.addVertex();
            try {
                v.addEdge("", v);
                fail("Call to Vertex.addEdge() should throw an exception when label is empty");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeEmpty(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingSystemVertexLabel() {
            final String label = Graph.Hidden.hide("systemLabel");
            final Vertex v = graph.addVertex();
            try {
                v.addEdge(label, v);
                fail("Call to Vertex.addEdge() should throw an exception when label is a system key");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeAHiddenKey(label), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldAutotypeStringProperties() {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "string", "marko");
            final String name = e.value("string");
            assertEquals(name, "marko");

        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        public void shouldAutotypeIntegerProperties() {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "integer", 33);
            final Integer age = e.value("integer");
            assertEquals(Integer.valueOf(33), age);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_BOOLEAN_VALUES)
        public void shouldAutotypeBooleanProperties() {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "boolean", true);
            final Boolean best = e.value("boolean");
            assertEquals(best, true);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_DOUBLE_VALUES)
        public void shouldAutotypeDoubleProperties() {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "double", 0.1d);
            final Double best = e.value("double");
            assertEquals(best, Double.valueOf(0.1d));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_LONG_VALUES)
        public void shouldAutotypeLongProperties() {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "long", 1l);
            final Long best = e.value("long");
            assertEquals(best, Long.valueOf(1l));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
        public void shouldAutotypeFloatProperties() {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "float", 0.1f);
            final Float best = e.value("float");
            assertEquals(best, Float.valueOf(0.1f));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldGetPropertyKeysOnEdge() {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("friend", v, "name", "marko", "location", "desert", "status", "dope");
            Set<String> keys = e.keys();
            assertEquals(3, keys.size());

            assertTrue(keys.contains("name"));
            assertTrue(keys.contains("location"));
            assertTrue(keys.contains("status"));

            final List<Property<Object>> m = IteratorUtils.list(e.properties());
            assertEquals(3, m.size());
            assertTrue(m.stream().anyMatch(p -> p.key().equals("name")));
            assertTrue(m.stream().anyMatch(p -> p.key().equals("location")));
            assertTrue(m.stream().anyMatch(p -> p.key().equals("status")));
            assertEquals("marko", m.stream().filter(p -> p.key().equals("name")).map(Property::value).findAny().orElse(null));
            assertEquals("desert", m.stream().filter(p -> p.key().equals("location")).map(Property::value).findAny().orElse(null));
            assertEquals("dope", m.stream().filter(p -> p.key().equals("status")).map(Property::value).findAny().orElse(null));

            e.property("status").remove();

            keys = e.keys();
            assertEquals(2, keys.size());
            assertTrue(keys.contains("name"));
            assertTrue(keys.contains("location"));

            e.properties().forEachRemaining(Property::remove);

            keys = e.keys();
            assertEquals(0, keys.size());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_REMOVE_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldNotGetConcurrentModificationException() {
            for (int i = 0; i < 25; i++) {
                final Vertex v = graph.addVertex();
                v.addEdge("friend", v);
            }

            tryCommit(graph, getAssertVertexEdgeCounts(25, 25));

            for (Edge e : g.E().toList()) {
                e.remove();
                tryCommit(graph);
            }

            tryCommit(graph, getAssertVertexEdgeCounts(25, 0));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_REMOVE_EDGES)
        public void shouldNotHaveAConcurrentModificationExceptionWhenIteratingAndRemovingAddingEdges() {
            final Vertex v1 = graph.addVertex("name", "marko");
            final Vertex v2 = graph.addVertex("name", "puppy");
            v1.addEdge("knows", v2, "since", 2010);
            v1.addEdge("pets", v2);
            v1.addEdge("walks", v2, "location", "arroyo");
            v2.addEdge("knows", v1, "since", 2010);
            assertEquals(4l, IteratorUtils.count(v1.edges(Direction.BOTH)));
            assertEquals(4l, IteratorUtils.count(v2.edges(Direction.BOTH)));
            v1.edges(Direction.BOTH).forEachRemaining(edge -> {
                v1.addEdge("livesWith", v2);
                v1.addEdge("walks", v2, "location", "river");
                edge.remove();
            });
            //assertEquals(8, v1.outE().count().next().intValue());  TODO: Neo4j is not happy
            //assertEquals(8, v2.outE().count().next().intValue());
            v1.edges(Direction.BOTH).forEachRemaining(Edge::remove);
            assertEquals(0, IteratorUtils.count(v1.edges(Direction.BOTH)));
            assertEquals(0, IteratorUtils.count(v2.edges(Direction.BOTH)));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldReturnEmptyIteratorIfNoProperties() {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v);
            assertEquals(0, IteratorUtils.count(e.properties()));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldReturnOutThenInOnVertexIterator() {
            final Vertex a = graph.addVertex();
            final Vertex b = graph.addVertex();
            final Edge e = a.addEdge("knows", b);
            assertEquals(a, e.outVertex());
            assertEquals(b, e.inVertex());
            final Iterator<Vertex> iterator = e.vertices(Direction.BOTH);
            assertTrue(iterator.hasNext());
            assertEquals(a, iterator.next());
            assertTrue(iterator.hasNext());
            assertEquals(b, iterator.next());
            assertFalse(iterator.hasNext());
        }
    }
}
