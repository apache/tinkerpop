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
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.util.CustomId;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.*;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class VertexTest {

    public static class AddEdgeTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
        public void shouldAddEdgeWithUserSuppliedNumericId() {
            final Vertex v = graph.addVertex();
            v.addEdge("self", v, T.id, 1000l);
            tryCommit(graph, graph -> {
                final Edge e = graph.edges(1000l).next();
                assertEquals(1000l, e.id());
            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_STRING_IDS)
        public void shouldAddEdgeWithUserSuppliedStringId() {
            final Vertex v = graph.addVertex();
            v.addEdge("self", v, T.id, "1000");
            tryCommit(graph, graph -> {
                final Edge e = graph.edges("1000").next();
                assertEquals("1000", e.id());
            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_UUID_IDS)
        public void shouldAddEdgeWithUserSuppliedUuidId() {
            final UUID uuid = UUID.randomUUID();
            final Vertex v = graph.addVertex();
            v.addEdge("self", v, T.id, uuid);
            tryCommit(graph, graph -> {
                final Edge e = graph.edges(uuid).next();
                assertEquals(uuid, e.id());
            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ANY_IDS)
        public void shouldAddEdgeWithUserSuppliedAnyIdUsingUuid() {
            final UUID uuid = UUID.randomUUID();
            final Vertex v = graph.addVertex();
            v.addEdge("self", v, T.id, uuid);
            tryCommit(graph, graph -> {
                final Edge e = graph.edges(uuid).next();
                assertEquals(uuid, e.id());
            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ANY_IDS)
        public void shouldAddEdgeWithUserSuppliedAnyIdUsingString() {
            final UUID uuid = UUID.randomUUID();
            final Vertex v = graph.addVertex();
            v.addEdge("self", v, T.id, uuid.toString());
            tryCommit(graph, graph -> {
                final Edge e = graph.edges(uuid.toString()).next();
                assertEquals(uuid.toString(), e.id());
            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ANY_IDS)
        public void shouldAddEdgeWithUserSuppliedAnyIdUsingAnyObject() {
            final UUID uuid = UUID.randomUUID();

            // this is different from "FEATURE_CUSTOM_IDS" as TinkerGraph does not define a specific id class
            // (i.e. TinkerId) for the identifier.
            final CustomId customId = new CustomId("test", uuid);
            final Vertex v = graph.addVertex();
            v.addEdge("self", v, T.id, customId);
            tryCommit(graph, graph -> {
                final Edge e = graph.edges(customId).next();
                assertEquals(customId, e.id());
            });
        }
    }

    @ExceptionCoverage(exceptionClass = Edge.Exceptions.class, methods = {
            "userSuppliedIdsNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
            "edgeWithIdAlreadyExists"
    })
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "labelCanNotBeNull",
            "labelCanNotBeEmpty",
            "labelCanNotBeAHiddenKey"
    })
    public static class BasicVertexTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldValidateEquality() {
            final Vertex v1 = graph.addVertex();
            final Vertex v2 = graph.addVertex();

            assertEquals(v1, v1);
            assertEquals(v2, v2);
            assertNotEquals(v1, v2);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldValidateIdEquality() {
            final Vertex v1 = graph.addVertex();
            final Vertex v2 = graph.addVertex();

            assertEquals(v1.id(), v1.id());
            assertEquals(v2.id(), v2.id());
            assertEquals(v1.id().toString(), v1.id().toString());
            assertEquals(v2.id().toString(), v2.id().toString());
            assertNotEquals(v1.id(), v2.id());
            assertNotEquals(v1.id().toString(), v2.id().toString());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingNullVertexLabel() {
            try {
                graph.addVertex(T.label, null);
                fail("Call to Graph.addVertex() should throw an exception when label is null");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeNull(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingNullVertexLabelOnOverload() {
            try {
                graph.addVertex((String) null);
                fail("Call to Graph.addVertex() should throw an exception when label is null");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeNull(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingEmptyVertexLabel() {
            try {
                graph.addVertex(T.label, "");
                fail("Call to Graph.addVertex() should throw an exception when label is empty");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeEmpty(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingEmptyVertexLabelOnOverload() {
            try {
                graph.addVertex("");
                fail("Call to Graph.addVertex() should throw an exception when label is empty");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeEmpty(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingSystemVertexLabel() {
            final String label = Graph.Hidden.hide("systemLabel");
            try {
                graph.addVertex(T.label, label);
                fail("Call to Graph.addVertex() should throw an exception when label is a system key");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeAHiddenKey(label), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingSystemVertexLabelOnOverload() {
            final String label = Graph.Hidden.hide("systemLabel");
            try {
                graph.addVertex(label);
                fail("Call to Graph.addVertex() should throw an exception when label is a system key");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeAHiddenKey(label), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        public void shouldHaveExceptionConsistencyWhenAssigningSameIdOnEdge() {
            final Vertex v = graph.addVertex();
            final Object o = graphProvider.convertId("1", Edge.class);
            v.addEdge("self", v, T.id, o);

            try {
                v.addEdge("self", v, T.id, o);
                fail("Assigning the same ID to an Element should throw an exception");
            } catch (Exception ex) {
                validateException(Graph.Exceptions.edgeWithIdAlreadyExists(o), ex);
            }

        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void shouldHaveExceptionConsistencyWhenIdNotSupportedForAddEdge() throws Exception {
            try {
                final Vertex v = this.graph.addVertex();
                v.addEdge("self", v, T.id, "");
                fail("Call to addEdge should have thrown an exception when ID was specified as it is not supported");
            } catch (Exception ex) {
                validateException(Edge.Exceptions.userSuppliedIdsNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        public void shouldHaveStandardStringRepresentation() {
            final Vertex v = graph.addVertex("name", "marko", "age", 34);
            assertEquals(StringFactory.vertexString(v), v.toString());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldUseDefaultLabelIfNotSpecified() {
            final Vertex v = graph.addVertex("name", "marko");
            assertEquals(Vertex.DEFAULT_LABEL, v.label());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldAddVertexWithLabel() {
            final Vertex v = graph.addVertex("person");
            this.tryCommit(graph, graph -> assertEquals("person", v.label()));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldSupportBasicVertexManipulation() {
            // test property mutation behaviors
            final Vertex v = graph.addVertex("name", "marko", "age", 34);
            assertEquals(34, (int) v.value("age"));
            assertEquals("marko", v.<String>value("name"));
            assertEquals(34, (int) v.property("age").value());
            assertEquals("marko", v.<String>property("name").value());
            assertEquals(2, IteratorUtils.count(v.properties()));
            assertEquals(2, v.keys().size());
            assertTrue(v.keys().contains("name"));
            assertTrue(v.keys().contains("age"));
            assertFalse(v.keys().contains("location"));
            assertVertexEdgeCounts(1, 0).accept(graph);

            v.properties("name").forEachRemaining(Property::remove);
            v.property(VertexProperty.Cardinality.single, "name", "marko rodriguez");
            assertEquals(34, (int) v.value("age"));
            assertEquals("marko rodriguez", v.<String>value("name"));
            assertEquals(34, (int) v.property("age").value());
            assertEquals("marko rodriguez", v.<String>property("name").value());
            assertEquals(2, IteratorUtils.count(v.properties()));
            assertEquals(2, v.keys().size());
            assertTrue(v.keys().contains("name"));
            assertTrue(v.keys().contains("age"));
            assertFalse(v.keys().contains("location"));
            assertVertexEdgeCounts(1, 0).accept(graph);

            v.property(VertexProperty.Cardinality.single, "location", "santa fe");
            assertEquals(3, IteratorUtils.count(v.properties()));
            assertEquals(3, v.keys().size());
            assertEquals("santa fe", v.property("location").value());
            assertEquals(v.property("location"), v.property("location"));
            assertNotEquals(v.property("location"), v.property("name"));
            assertTrue(v.keys().contains("name"));
            assertTrue(v.keys().contains("age"));
            assertTrue(v.keys().contains("location"));
            v.property("location").remove();
            assertVertexEdgeCounts(1, 0).accept(graph);
            assertEquals(2, IteratorUtils.count(v.properties()));
            v.properties().forEachRemaining(Property::remove);
            assertEquals(0, IteratorUtils.count(v.properties()));
            assertVertexEdgeCounts(1, 0).accept(graph);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        public void shouldEvaluateVerticesEquivalentWithSuppliedIdsViaTraversal() {
            final Vertex v = graph.addVertex(T.id, graphProvider.convertId("1", Vertex.class));
            final Vertex u = graph.vertices(graphProvider.convertId("1", Vertex.class)).next();
            assertEquals(v, u);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        public void shouldEvaluateVerticesEquivalentWithSuppliedIdsViaIterators() {
            final Vertex v = graph.addVertex(T.id, graphProvider.convertId("1", Vertex.class));
            final Vertex u = graph.vertices(graphProvider.convertId("1", Vertex.class)).next();
            assertEquals(v, u);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldEvaluateEquivalentVerticesWithNoSuppliedIds() {
            final Vertex v = graph.addVertex();
            assertNotNull(v);

            final Vertex u = graph.vertices(v.id()).next();
            assertNotNull(u);
            assertEquals(v, u);

            assertEquals(graph.vertices(u.id()).next(), graph.vertices(u.id()).next());
            assertEquals(graph.vertices(v.id()).next(), graph.vertices(u.id()).next());
            assertEquals(graph.vertices(v.id()).next(), graph.vertices(v.id()).next());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        public void shouldEvaluateEquivalentVertexHashCodeWithSuppliedIds() {
            final Vertex v = graph.addVertex(T.id, graphProvider.convertId("1", Vertex.class));
            final Vertex u = graph.vertices(graphProvider.convertId("1", Vertex.class)).next();
            assertEquals(v, u);

            final Set<Vertex> set = new HashSet<>();
            set.add(v);
            set.add(v);
            set.add(u);
            set.add(u);
            set.add(graph.vertices(graphProvider.convertId("1", Vertex.class)).next());
            set.add(graph.vertices(graphProvider.convertId("1", Vertex.class)).next());

            assertEquals(1, set.size());
            assertEquals(v.hashCode(), u.hashCode());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldAutotypeStringProperties() {
            final Vertex v = graph.addVertex();
            v.property(VertexProperty.Cardinality.single, "string", "marko");
            final String name = v.value("string");
            assertEquals(name, "marko");

        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        public void shouldAutotypIntegerProperties() {
            final Vertex v = graph.addVertex();
            v.property(VertexProperty.Cardinality.single, "integer", 33);
            final Integer age = v.value("integer");
            assertEquals(Integer.valueOf(33), age);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_BOOLEAN_VALUES)
        public void shouldAutotypeBooleanProperties() {
            final Vertex v = graph.addVertex();
            v.property(VertexProperty.Cardinality.single, "boolean", true);
            final Boolean best = v.value("boolean");
            assertEquals(best, true);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_DOUBLE_VALUES)
        public void shouldAutotypeDoubleProperties() {
            final Vertex v = graph.addVertex();
            v.property(VertexProperty.Cardinality.single, "double", 0.1d);
            final Double best = v.value("double");
            assertEquals(best, Double.valueOf(0.1d));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_LONG_VALUES)
        public void shouldAutotypeLongProperties() {
            final Vertex v = graph.addVertex();
            v.property(VertexProperty.Cardinality.single, "long", 1l);
            final Long best = v.value("long");
            assertEquals(best, Long.valueOf(1l));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
        public void shouldAutotypeFloatProperties() {
            final Vertex v = graph.addVertex();
            v.property(VertexProperty.Cardinality.single, "float", 0.1f);
            final Float best = v.value("float");
            assertEquals(best, Float.valueOf(0.1f));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldGetPropertyKeysOnVertex() {
            final Vertex v = graph.addVertex("name", "marko", "location", "desert", "status", "dope");
            Set<String> keys = v.keys();
            assertEquals(3, keys.size());

            assertTrue(keys.contains("name"));
            assertTrue(keys.contains("location"));
            assertTrue(keys.contains("status"));

            final List<VertexProperty<Object>> m = IteratorUtils.list(v.properties());
            assertEquals(3, m.size());
            assertTrue(m.stream().anyMatch(p -> p.key().equals("name")));
            assertTrue(m.stream().anyMatch(p -> p.key().equals("location")));
            assertTrue(m.stream().anyMatch(p -> p.key().equals("status")));
            assertEquals("marko", m.stream().filter(p -> p.key().equals("name")).map(Property::value).findAny().orElse(null));
            assertEquals("desert", m.stream().filter(p -> p.key().equals("location")).map(Property::value).findAny().orElse(null));
            assertEquals("dope", m.stream().filter(p -> p.key().equals("status")).map(Property::value).findAny().orElse(null));

            v.property("status").remove();

            keys = v.keys();
            assertEquals(2, keys.size());
            assertTrue(keys.contains("name"));
            assertTrue(keys.contains("location"));

            v.properties().forEachRemaining(Property::remove);

            keys = v.keys();
            assertEquals(0, keys.size());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
        public void shouldNotGetConcurrentModificationException() {
            for (int i = 0; i < 25; i++) {
                graph.addVertex("myId", i);
            }
            graph.vertices().forEachRemaining(v -> graph.vertices().forEachRemaining(u -> v.addEdge("knows", u, "myEdgeId", 12)));

            tryCommit(graph, assertVertexEdgeCounts(25, 625));

            final List<Vertex> vertices = new ArrayList<>();
            IteratorUtils.fill(graph.vertices(), vertices);
            for (Vertex v : vertices) {
                v.remove();
                tryCommit(graph);
            }

            tryCommit(graph, assertVertexEdgeCounts(0, 0));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldReturnEmptyIteratorIfNoProperties() {
            final Vertex v = graph.addVertex();
            assertEquals(0, IteratorUtils.count(v.properties()));
        }
    }
}
