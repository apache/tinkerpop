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
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Serialization tests that occur at a lower level than IO.  Note that there is no need to test GraphML here as
 * it is not a format that can be used for generalized serialization (i.e. it only serializes an entire graph).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class SerializationTest {
    public static class GryoV1d0Test extends AbstractGremlinTest {
        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V1_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final Vertex v = graph.vertices(convertToVertexId("marko")).next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, v);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final Vertex detached = gryoReader.readObject(inputStream, DetachedVertex.class);
            assertNotNull(detached);
            assertEquals(v.label(), detached.label());
            assertEquals(v.id(), detached.id());
            assertEquals(v.value("name").toString(), detached.value("name"));
            assertEquals((Integer) v.value("age"), detached.value("age"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeEdgeAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V1_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final Edge e = g.E(convertToEdgeId("marko", "knows", "vadas")).next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, e);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final Edge detached = gryoReader.readObject(inputStream, DetachedEdge.class);
            assertNotNull(detached);
            assertEquals(e.label(), detached.label());
            assertEquals(e.id(), detached.id());
            assertEquals((Double) e.value("weight"), detached.value("weight"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializePropertyAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V1_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final Property property = g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("weight");
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, property);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final Property detached = gryoReader.readObject(inputStream, DetachedProperty.class);
            assertNotNull(detached);
            assertEquals(property.key(), detached.key());
            assertEquals(property.value(), detached.value());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexPropertyAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V1_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final VertexProperty vertexProperty = graph.vertices(convertToVertexId("marko")).next().property("name");
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, vertexProperty);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final VertexProperty detached = gryoReader.readObject(inputStream, DetachedVertexProperty.class);
            assertNotNull(detached);
            assertEquals(vertexProperty.label(), detached.label());
            assertEquals(vertexProperty.id(), detached.id());
            assertEquals(vertexProperty.value(), detached.value());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.CREW)
        public void shouldSerializeVertexPropertyWithPropertiesAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V1_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final VertexProperty<?> vertexProperty = IteratorUtils.filter(graph.vertices(convertToVertexId("marko")).next().properties("location"), p -> p.value().equals("brussels")).next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, vertexProperty);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final VertexProperty<?> detached = gryoReader.readObject(inputStream, DetachedVertexProperty.class);

            assertNotNull(detached);
            assertEquals(vertexProperty.label(), detached.label());
            assertEquals(vertexProperty.id(), detached.id());
            assertEquals(vertexProperty.value(), detached.value());
            assertEquals(vertexProperty.values("startTime").next(), detached.values("startTime").next());
            assertEquals(vertexProperty.properties("startTime").next().key(), detached.properties("startTime").next().key());
            assertEquals(vertexProperty.values("endTime").next(), detached.values("endTime").next());
            assertEquals(vertexProperty.properties("endTime").next().key(), detached.properties("endTime").next().key());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializePathAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V1_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final Path p = g.V(convertToVertexId("marko")).as("a").outE().as("b").inV().as("c").path()
                    .filter(t -> ((Vertex) t.get().objects().get(2)).value("name").equals("lop")).next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, p);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final Path detached = gryoReader.readObject(inputStream, DetachedPath.class);
            assertNotNull(detached);
            assertEquals(p.labels().size(), detached.labels().size());
            assertEquals(p.labels().get(0).size(), detached.labels().get(0).size());
            assertEquals(p.labels().get(1).size(), detached.labels().get(1).size());
            assertEquals(p.labels().get(2).size(), detached.labels().get(2).size());
            assertTrue(p.labels().stream().flatMap(Collection::stream).allMatch(detached::hasLabel));

            final Vertex vOut = p.get("a");
            final Vertex detachedVOut = detached.get("a");
            assertEquals(vOut.label(), detachedVOut.label());
            assertEquals(vOut.id(), detachedVOut.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedVOut.properties().hasNext());

            final Edge e = p.get("b");
            final Edge detachedE = detached.get("b");
            assertEquals(e.label(), detachedE.label());
            assertEquals(e.id(), detachedE.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedE.properties().hasNext());

            final Vertex vIn = p.get("c");
            final Vertex detachedVIn = detached.get("c");
            assertEquals(vIn.label(), detachedVIn.label());
            assertEquals(vIn.id(), detachedVIn.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedVIn.properties().hasNext());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeTraversalMetrics() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V1_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final TraversalMetrics before = g.V().both().profile().next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, before);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final TraversalMetrics after = gryoReader.readObject(inputStream, TraversalMetrics.class);
            assertNotNull(after);
            assertEquals(before.getMetrics().size(), after.getMetrics().size());
            assertEquals(before.getDuration(TimeUnit.MILLISECONDS), after.getDuration(TimeUnit.MILLISECONDS));
            assertEquals(before.getMetrics(0).getCounts(), after.getMetrics().stream().findFirst().get().getCounts());
        }
        
        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeTree() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V1_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final Tree before = g.V(convertToVertexId("marko")).out().properties("name").tree().next();
            
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, before);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final Tree after = gryoReader.readObject(inputStream, Tree.class);
            assertNotNull(after);
            //The following assertions should be sufficent.
            assertThat("Type does not match", after, instanceOf(Tree.class));
            assertEquals("The objects differ", after, before);
        }
    }

    public static class GryoV3d0Test extends AbstractGremlinTest {
        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V3_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final Vertex v = graph.vertices(convertToVertexId("marko")).next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, v);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final Vertex detached = gryoReader.readObject(inputStream, DetachedVertex.class);
            assertNotNull(detached);
            assertEquals(v.label(), detached.label());
            assertEquals(v.id(), detached.id());
            assertEquals(v.value("name").toString(), detached.value("name"));
            assertEquals((Integer) v.value("age"), detached.value("age"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeEdgeAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V3_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final Edge e = g.E(convertToEdgeId("marko", "knows", "vadas")).next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, e);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final Edge detached = gryoReader.readObject(inputStream, DetachedEdge.class);
            assertNotNull(detached);
            assertEquals(e.label(), detached.label());
            assertEquals(e.id(), detached.id());
            assertEquals((Double) e.value("weight"), detached.value("weight"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializePropertyAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V3_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final Property property = g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("weight");
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, property);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final Property detached = gryoReader.readObject(inputStream, DetachedProperty.class);
            assertNotNull(detached);
            assertEquals(property.key(), detached.key());
            assertEquals(property.value(), detached.value());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexPropertyAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V3_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final VertexProperty vertexProperty = graph.vertices(convertToVertexId("marko")).next().property("name");
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, vertexProperty);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final VertexProperty detached = gryoReader.readObject(inputStream, DetachedVertexProperty.class);
            assertNotNull(detached);
            assertEquals(vertexProperty.label(), detached.label());
            assertEquals(vertexProperty.id(), detached.id());
            assertEquals(vertexProperty.value(), detached.value());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.CREW)
        public void shouldSerializeVertexPropertyWithPropertiesAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V3_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final VertexProperty<?> vertexProperty = IteratorUtils.filter(graph.vertices(convertToVertexId("marko")).next().properties("location"), p -> p.value().equals("brussels")).next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, vertexProperty);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final VertexProperty<?> detached = gryoReader.readObject(inputStream, DetachedVertexProperty.class);

            assertNotNull(detached);
            assertEquals(vertexProperty.label(), detached.label());
            assertEquals(vertexProperty.id(), detached.id());
            assertEquals(vertexProperty.value(), detached.value());
            assertEquals(vertexProperty.values("startTime").next(), detached.values("startTime").next());
            assertEquals(vertexProperty.properties("startTime").next().key(), detached.properties("startTime").next().key());
            assertEquals(vertexProperty.values("endTime").next(), detached.values("endTime").next());
            assertEquals(vertexProperty.properties("endTime").next().key(), detached.properties("endTime").next().key());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializePathAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V3_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final Path p = g.V(convertToVertexId("marko")).as("a").outE().as("b").inV().as("c").path()
                    .filter(t -> ((Vertex) t.get().objects().get(2)).value("name").equals("lop")).next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, p);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final Path detached = gryoReader.readObject(inputStream, DetachedPath.class);
            assertNotNull(detached);
            assertEquals(p.labels().size(), detached.labels().size());
            assertEquals(p.labels().get(0).size(), detached.labels().get(0).size());
            assertEquals(p.labels().get(1).size(), detached.labels().get(1).size());
            assertEquals(p.labels().get(2).size(), detached.labels().get(2).size());
            assertTrue(p.labels().stream().flatMap(Collection::stream).allMatch(detached::hasLabel));

            final Vertex vOut = p.get("a");
            final Vertex detachedVOut = detached.get("a");
            assertEquals(vOut.label(), detachedVOut.label());
            assertEquals(vOut.id(), detachedVOut.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedVOut.properties().hasNext());

            final Edge e = p.get("b");
            final Edge detachedE = detached.get("b");
            assertEquals(e.label(), detachedE.label());
            assertEquals(e.id(), detachedE.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedE.properties().hasNext());

            final Vertex vIn = p.get("c");
            final Vertex detachedVIn = detached.get("c");
            assertEquals(vIn.label(), detachedVIn.label());
            assertEquals(vIn.id(), detachedVIn.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedVIn.properties().hasNext());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeTraversalMetrics() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V3_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final TraversalMetrics before = (TraversalMetrics) g.V().both().profile().next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, before);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final TraversalMetrics after = gryoReader.readObject(inputStream, TraversalMetrics.class);
            assertNotNull(after);
            assertEquals(before.getMetrics().size(), after.getMetrics().size());
            assertEquals(before.getDuration(TimeUnit.MILLISECONDS), after.getDuration(TimeUnit.MILLISECONDS));
            assertEquals(before.getMetrics(0).getCounts(), after.getMetrics().stream().findFirst().get().getCounts());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeTree() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build(GryoVersion.V3_0));
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final Tree before = g.V(convertToVertexId("marko")).out().properties("name").tree().next();

            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, before);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final Tree after = gryoReader.readObject(inputStream, Tree.class);
            assertNotNull(after);
            //The following assertions should be sufficent.
            assertThat("Type does not match", after, instanceOf(Tree.class));
            assertEquals("The objects differ", after, before);
        }
    }

    public static class GraphSONV1d0Test extends AbstractGremlinTest {
        private final TypeReference<HashMap<String, Object>> mapTypeReference = new TypeReference<HashMap<String, Object>>() {
        };

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertex() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().version(GraphSONVersion.V1_0).create().createMapper();
            final Vertex v = graph.vertices(convertToVertexId("marko")).next();
            final String json = mapper.writeValueAsString(v);
            final Map<String, Object> m = mapper.readValue(json, mapTypeReference);

            assertEquals(GraphSONTokens.VERTEX, m.get(GraphSONTokens.TYPE));
            assertEquals(v.label(), m.get(GraphSONTokens.LABEL));
            assertNotNull(m.get(GraphSONTokens.ID));
            final Map<String,List<Map<String,Object>>> properties = (Map<String,List<Map<String,Object>>>) m.get(GraphSONTokens.PROPERTIES);
            assertEquals(v.value("name").toString(), properties.get("name").get(0).get(GraphSONTokens.VALUE).toString());
            assertEquals((Integer) v.value("age"), properties.get("age").get(0).get(GraphSONTokens.VALUE));
            assertEquals(1, properties.get("name").size());
            assertEquals(1, properties.get("age").size());
            assertEquals(2, properties.size());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeEdge() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().version(GraphSONVersion.V1_0).create().createMapper();
            final Edge e = g.E(convertToEdgeId("marko", "knows", "vadas")).next();
            final String json = mapper.writeValueAsString(e);
            final Map<String, Object> m = mapper.readValue(json, mapTypeReference);

            assertEquals(GraphSONTokens.EDGE, m.get(GraphSONTokens.TYPE));
            assertEquals(e.label(), m.get(GraphSONTokens.LABEL));
            assertNotNull(m.get(GraphSONTokens.ID));
            assertEquals((Double) e.value("weight"), ((Map) m.get(GraphSONTokens.PROPERTIES)).get("weight"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeProperty() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().version(GraphSONVersion.V1_0).create().createMapper();
            final Property p = g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("weight");
            final String json = mapper.writeValueAsString(p);
            final Map<String, Object> m = mapper.readValue(json, mapTypeReference);

            assertEquals(p.value(), m.get(GraphSONTokens.VALUE));
            assertEquals(p.key(), m.get(GraphSONTokens.KEY));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexProperty() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().version(GraphSONVersion.V1_0).create().createMapper();
            final VertexProperty vp = graph.vertices(convertToVertexId("marko")).next().property("name");
            final String json = mapper.writeValueAsString(vp);
            final Map<String, Object> m = mapper.readValue(json, mapTypeReference);

            assertEquals(vp.label(), m.get(GraphSONTokens.LABEL));
            assertNotNull(m.get(GraphSONTokens.ID));
            assertEquals(vp.value(), m.get(GraphSONTokens.VALUE));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.CREW)
        public void shouldSerializeVertexPropertyWithProperties() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().version(GraphSONVersion.V1_0).create().createMapper();
            final VertexProperty vp = IteratorUtils.filter(graph.vertices(convertToVertexId("marko")).next().properties("location"), p -> p.value().equals("brussels")).next();
            final String json = mapper.writeValueAsString(vp);
            final Map<String, Object> m = mapper.readValue(json, mapTypeReference);

            assertEquals(vp.label(), m.get(GraphSONTokens.LABEL));
            assertNotNull(m.get(GraphSONTokens.ID));
            assertEquals(vp.value(), m.get(GraphSONTokens.VALUE));
            assertEquals(vp.values("startTime").next(), ((Map) m.get(GraphSONTokens.PROPERTIES)).get("startTime"));
            assertEquals(vp.values("endTime").next(), ((Map) m.get(GraphSONTokens.PROPERTIES)).get("endTime"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializePath() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().version(GraphSONVersion.V1_0).create().createMapper();
            final Path p = g.V(convertToVertexId("marko")).as("a").outE().as("b").inV().as("c").path()
                    .filter(t -> ((Vertex) t.get().objects().get(2)).value("name").equals("lop")).next();
            final String json = mapper.writeValueAsString(p);
            final Map<String, Object> m = mapper.readValue(json, mapTypeReference);

            assertEquals(2, m.size());

            final List<List<String>> labels = (List<List<String>>) m.get(GraphSONTokens.LABELS);
            assertEquals(3, labels.size());
            assertEquals("a", labels.get(0).get(0));
            assertEquals(1, labels.get(0).size());
            assertEquals("b", labels.get(1).get(0));
            assertEquals(1, labels.get(1).size());
            assertEquals("c", labels.get(2).get(0));
            assertEquals(1, labels.get(2).size());

            final List<Object> objects = (List<Object>) m.get(GraphSONTokens.OBJECTS);
            assertEquals(3, objects.size());

            assertEquals("marko", ((List<Map>) ((Map) ((Map) objects.get(0)).get(GraphSONTokens.PROPERTIES)).get("name")).get(0).get(GraphSONTokens.VALUE).toString());
            assertEquals("created", ((Map) objects.get(1)).get(GraphSONTokens.LABEL));
            assertEquals("lop", ((List<Map>) ((Map) ((Map) objects.get(2)).get(GraphSONTokens.PROPERTIES)).get("name")).get(0).get(GraphSONTokens.VALUE).toString());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeTraversalMetrics() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().version(GraphSONVersion.V1_0).create().createMapper();
            final TraversalMetrics tm = g.V().both().profile().next();
            final String json = mapper.writeValueAsString(tm);
            final Map<String, Object> m = mapper.readValue(json, mapTypeReference);

            assertTrue(m.containsKey(GraphSONTokens.DURATION));
            assertTrue(m.containsKey(GraphSONTokens.METRICS));

            final List<Map<String, Object>> metrics = (List<Map<String, Object>>) m.get(GraphSONTokens.METRICS);
            assertEquals(tm.getMetrics().size(), metrics.size());

            final Map<String, Object> metrics0 = metrics.get(0);
            assertTrue(metrics0.containsKey(GraphSONTokens.ID));
            assertTrue(metrics0.containsKey(GraphSONTokens.NAME));
            assertTrue(metrics0.containsKey(GraphSONTokens.COUNTS));
            assertTrue(metrics0.containsKey(GraphSONTokens.DURATION));
        }
        
        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeTree() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().version(GraphSONVersion.V1_0).create().createMapper();
            final Tree t = g.V(convertToVertexId("marko")).out().properties("name").tree().next();
            final String json = mapper.writeValueAsString(t);

            final HashMap<String, Object> m = (HashMap<String, Object>) mapper.readValue(json, mapTypeReference);

            // Check Structure
            assertEquals(1, m.size());
            assertTrue(m.containsKey(convertToVertexId("marko").toString()));

            // Check Structure n+1
            final HashMap<String, Object> branch = (HashMap<String, Object>) m.get(convertToVertexId("marko").toString());
            assertEquals(2, branch.size());
            assertTrue(branch.containsKey(GraphSONTokens.KEY));
            assertTrue(branch.containsKey(GraphSONTokens.VALUE));

            //Check n+1 key (traversed element)
            final HashMap<String, Object> branchKey = (HashMap<String, Object>) branch.get(GraphSONTokens.KEY);
            assertTrue(branchKey.containsKey(GraphSONTokens.ID));
            assertTrue(branchKey.containsKey(GraphSONTokens.LABEL));
            assertTrue(branchKey.containsKey(GraphSONTokens.TYPE));
            assertTrue(branchKey.containsKey(GraphSONTokens.PROPERTIES));
            assertEquals(convertToVertexId("marko").toString(), branchKey.get(GraphSONTokens.ID).toString());
            assertEquals("person", branchKey.get(GraphSONTokens.LABEL));
            assertEquals("vertex", branchKey.get(GraphSONTokens.TYPE));
            final HashMap<String, List<HashMap<String, Object>>> branchKeyProps = (HashMap<String, List<HashMap<String, Object>>>) branchKey.get(GraphSONTokens.PROPERTIES);
            assertEquals("marko", branchKeyProps.get("name").get(0).get("value"));
            assertEquals(29, branchKeyProps.get("age").get(0).get("value"));

            //Check n+1 value (traversed element)
            final HashMap<String, Object> branchValue = (HashMap<String, Object>) branch.get(GraphSONTokens.VALUE);
            assertEquals(3, branchValue.size());
            assertTrue(branchValue.containsKey(convertToVertexId("vadas").toString()));
            assertTrue(branchValue.containsKey(convertToVertexId("lop").toString()));
            assertTrue(branchValue.containsKey(convertToVertexId("josh").toString()));

            // Check that vp[] functioned properly
            final HashMap<String, HashMap<String, Object>> branch2 = (HashMap<String, HashMap<String, Object>>) branchValue.get(convertToVertexId("vadas").toString());
            assertTrue(branch2.containsKey(GraphSONTokens.KEY));
            assertTrue(branch2.containsKey(GraphSONTokens.VALUE));

            final Map.Entry entry = branch2.get(GraphSONTokens.VALUE).entrySet().iterator().next();
            final HashMap<String, HashMap<String, Object>> branch2Prop = (HashMap<String, HashMap<String, Object>>) entry.getValue();
            assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.ID));
            assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.VALUE));
            assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.LABEL));
            assertEquals("name", branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.LABEL));
            assertEquals("vadas", branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.VALUE));
            assertEquals(entry.getKey().toString(), branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.ID).toString());
        }
    }

    public static class GraphSONV2d0Test extends AbstractGremlinTest {
        private final TypeReference<HashMap<String, Object>> mapTypeReference = new TypeReference<HashMap<String, Object>>() {
        };

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertex() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().version(GraphSONVersion.V2_0).create().createMapper();
            final Vertex v = graph.vertices(convertToVertexId("marko")).next();
            final String json = mapper.writeValueAsString(v);
            final Vertex detached = mapper.readValue(json, Vertex.class);

            assertNotNull(detached);
            assertEquals(v.label(), detached.label());
            assertEquals(v.id(), detached.id());
            assertEquals(v.value("name").toString(), detached.value("name"));
            assertEquals((Integer) v.value("age"), detached.value("age"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeEdge() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().version(GraphSONVersion.V2_0).create().createMapper();
            final Edge e = g.E(convertToEdgeId("marko", "knows", "vadas")).next();
            final String json = mapper.writeValueAsString(e);
            final Edge detached = mapper.readValue(json, Edge.class);

            assertNotNull(detached);
            assertEquals(e.label(), detached.label());
            assertEquals(e.id(), detached.id());
            assertEquals((Double) e.value("weight"), detached.value("weight"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeProperty() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().version(GraphSONVersion.V2_0).create().createMapper();
            final Property p = g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("weight");
            final String json = mapper.writeValueAsString(p);
            final Property detached = mapper.readValue(json, Property.class);

            assertNotNull(detached);
            assertEquals(p.key(), detached.key());
            assertEquals(p.value(), detached.value());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexProperty() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().version(GraphSONVersion.V2_0).create().createMapper();
            final VertexProperty vp = graph.vertices(convertToVertexId("marko")).next().property("name");
            final String json = mapper.writeValueAsString(vp);
            final VertexProperty detached = mapper.readValue(json, VertexProperty.class);

            assertNotNull(detached);
            assertEquals(vp.label(), detached.label());
            assertEquals(vp.id(), detached.id());
            assertEquals(vp.value(), detached.value());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.CREW)
        public void shouldSerializeVertexPropertyWithProperties() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().version(GraphSONVersion.V2_0).create().createMapper();
            final VertexProperty vp = IteratorUtils.filter(graph.vertices(convertToVertexId("marko")).next().properties("location"), p -> p.value().equals("brussels")).next();
            final String json = mapper.writeValueAsString(vp);
            final VertexProperty<?> detached = mapper.readValue(json, VertexProperty.class);

            assertNotNull(detached);
            assertEquals(vp.label(), detached.label());
            assertEquals(vp.id(), detached.id());
            assertEquals(vp.value(), detached.value());
            assertEquals(vp.values("startTime").next(), detached.values("startTime").next());
            assertEquals(((Property) vp.properties("startTime").next()).key(), ((Property) detached.properties("startTime").next()).key());
            assertEquals(vp.values("endTime").next(), detached.values("endTime").next());
            assertEquals(((Property) vp.properties("endTime").next()).key(), ((Property) detached.properties("endTime").next()).key());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializePath() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().version(GraphSONVersion.V2_0).create().createMapper();
            final Path p = g.V(convertToVertexId("marko")).as("a").outE().as("b").inV().as("c").path()
                    .filter(t -> ((Vertex) t.get().objects().get(2)).value("name").equals("lop")).next();
            final String json = mapper.writeValueAsString(p);
            final Path detached = mapper.readValue(json, Path.class);

            assertNotNull(detached);
            assertEquals(p.labels().size(), detached.labels().size());
            assertEquals(p.labels().get(0).size(), detached.labels().get(0).size());
            assertEquals(p.labels().get(1).size(), detached.labels().get(1).size());
            assertEquals(p.labels().get(2).size(), detached.labels().get(2).size());
            assertTrue(p.labels().stream().flatMap(Collection::stream).allMatch(detached::hasLabel));

            final Vertex vOut = p.get("a");
            final Vertex detachedVOut = detached.get("a");
            assertEquals(vOut.label(), detachedVOut.label());
            assertEquals(vOut.id(), detachedVOut.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedVOut.properties().hasNext());

            final Edge e = p.get("b");
            final Edge detachedE = detached.get("b");
            assertEquals(e.label(), detachedE.label());
            assertEquals(e.id(), detachedE.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedE.properties().hasNext());

            final Vertex vIn = p.get("c");
            final Vertex detachedVIn = detached.get("c");
            assertEquals(vIn.label(), detachedVIn.label());
            assertEquals(vIn.id(), detachedVIn.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedVIn.properties().hasNext());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeTraversalMetrics() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().version(GraphSONVersion.V2_0).create().createMapper();
            final TraversalMetrics before = g.V().both().profile().next();
            final String json = mapper.writeValueAsString(before);
            final TraversalMetrics after = mapper.readValue(json, TraversalMetrics.class);

            assertNotNull(after);
            assertEquals(before.getMetrics().size(), after.getMetrics().size());
            assertEquals(before.getDuration(TimeUnit.MILLISECONDS), after.getDuration(TimeUnit.MILLISECONDS));
            assertEquals(before.getMetrics().size(), after.getMetrics().size());

            before.getMetrics().forEach(b -> {
                final Optional<? extends Metrics> mFromA = after.getMetrics().stream().filter(a -> b.getId().equals(a.getId())).findFirst();
                if (mFromA.isPresent()) {
                    final Metrics m = mFromA.get();
                    assertEquals(b.getAnnotations(), m.getAnnotations());
                    assertEquals(b.getCounts(), m.getCounts());
                    assertEquals(b.getName(), m.getName());
                    assertEquals(b.getDuration(TimeUnit.MILLISECONDS), m.getDuration(TimeUnit.MILLISECONDS));
                } else {
                    fail("Metrics were not present after deserialization");
                }
            });
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        @org.junit.Ignore("TINKERPOP-1509")
        public void shouldSerializeTree() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().version(GraphSONVersion.V2_0).create().createMapper();
            final Tree t = g.V(convertToVertexId("marko")).out().properties("name").tree().next();
            final String json = mapper.writeValueAsString(t);

            final HashMap<String, Object> m = (HashMap<String, Object>) mapper.readValue(json, mapTypeReference);

            // Check Structure
            assertEquals(1, m.size());
            assertTrue(m.containsKey(convertToVertexId("marko").toString()));

            // Check Structure n+1
            final HashMap<String, Object> branch = (HashMap<String, Object>) m.get(convertToVertexId("marko").toString());
            assertEquals(2, branch.size());
            assertTrue(branch.containsKey(GraphSONTokens.KEY));
            assertTrue(branch.containsKey(GraphSONTokens.VALUE));

            //Check n+1 key (traversed element)
            final HashMap<String, Object> branchKey = (HashMap<String, Object>) branch.get(GraphSONTokens.KEY);
            assertTrue(branchKey.containsKey(GraphSONTokens.ID));
            assertTrue(branchKey.containsKey(GraphSONTokens.LABEL));
            assertTrue(branchKey.containsKey(GraphSONTokens.TYPE));
            assertTrue(branchKey.containsKey(GraphSONTokens.PROPERTIES));
            assertEquals(convertToVertexId("marko").toString(), branchKey.get(GraphSONTokens.ID).toString());
            assertEquals("person", branchKey.get(GraphSONTokens.LABEL));
            assertEquals("vertex", branchKey.get(GraphSONTokens.TYPE));
            final HashMap<String, List<HashMap<String, Object>>> branchKeyProps = (HashMap<String, List<HashMap<String, Object>>>) branchKey.get(GraphSONTokens.PROPERTIES);
            assertEquals("marko", branchKeyProps.get("name").get(0).get("value"));
            assertEquals(29, branchKeyProps.get("age").get(0).get("value"));

            //Check n+1 value (traversed element)
            final HashMap<String, Object> branchValue = (HashMap<String, Object>) branch.get(GraphSONTokens.VALUE);
            assertEquals(3, branchValue.size());
            assertTrue(branchValue.containsKey(convertToVertexId("vadas").toString()));
            assertTrue(branchValue.containsKey(convertToVertexId("lop").toString()));
            assertTrue(branchValue.containsKey(convertToVertexId("josh").toString()));

            // Check that vp[] functioned properly
            final HashMap<String, HashMap<String, Object>> branch2 = (HashMap<String, HashMap<String, Object>>) branchValue.get(convertToVertexId("vadas").toString());
            assertTrue(branch2.containsKey(GraphSONTokens.KEY));
            assertTrue(branch2.containsKey(GraphSONTokens.VALUE));

            final Map.Entry entry = branch2.get(GraphSONTokens.VALUE).entrySet().iterator().next();
            final HashMap<String, HashMap<String, Object>> branch2Prop = (HashMap<String, HashMap<String, Object>>) entry.getValue();
            assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.ID));
            assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.VALUE));
            assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.LABEL));
            assertEquals("name", branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.LABEL));
            assertEquals("vadas", branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.VALUE));
            assertEquals(entry.getKey().toString(), branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.ID).toString());
        }
    }

    public static class GraphSONV3d0Test extends AbstractGremlinTest {
        private final TypeReference<HashMap<String, Object>> mapTypeReference = new TypeReference<HashMap<String, Object>>() {
        };

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertex() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().version(GraphSONVersion.V3_0).create().createMapper();
            final Vertex v = graph.vertices(convertToVertexId("marko")).next();
            final String json = mapper.writeValueAsString(v);
            final Vertex detached = mapper.readValue(json, Vertex.class);

            assertNotNull(detached);
            assertEquals(v.label(), detached.label());
            assertEquals(v.id(), detached.id());
            assertEquals(v.value("name").toString(), detached.value("name"));
            assertEquals((Integer) v.value("age"), detached.value("age"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeEdge() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().version(GraphSONVersion.V3_0).create().createMapper();
            final Edge e = g.E(convertToEdgeId("marko", "knows", "vadas")).next();
            final String json = mapper.writeValueAsString(e);
            final Edge detached = mapper.readValue(json, Edge.class);

            assertNotNull(detached);
            assertEquals(e.label(), detached.label());
            assertEquals(e.id(), detached.id());
            assertEquals((Double) e.value("weight"), detached.value("weight"));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeProperty() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().version(GraphSONVersion.V3_0).create().createMapper();
            final Property p = g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("weight");
            final String json = mapper.writeValueAsString(p);
            final Property detached = mapper.readValue(json, Property.class);

            assertNotNull(detached);
            assertEquals(p.key(), detached.key());
            assertEquals(p.value(), detached.value());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexProperty() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().version(GraphSONVersion.V3_0).create().createMapper();
            final VertexProperty vp = graph.vertices(convertToVertexId("marko")).next().property("name");
            final String json = mapper.writeValueAsString(vp);
            final VertexProperty detached = mapper.readValue(json, VertexProperty.class);

            assertNotNull(detached);
            assertEquals(vp.label(), detached.label());
            assertEquals(vp.id(), detached.id());
            assertEquals(vp.value(), detached.value());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.CREW)
        public void shouldSerializeVertexPropertyWithProperties() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().version(GraphSONVersion.V3_0).create().createMapper();
            final VertexProperty vp = IteratorUtils.filter(graph.vertices(convertToVertexId("marko")).next().properties("location"), p -> p.value().equals("brussels")).next();
            final String json = mapper.writeValueAsString(vp);
            final VertexProperty<?> detached = mapper.readValue(json, VertexProperty.class);

            assertNotNull(detached);
            assertEquals(vp.label(), detached.label());
            assertEquals(vp.id(), detached.id());
            assertEquals(vp.value(), detached.value());
            assertEquals(vp.values("startTime").next(), detached.values("startTime").next());
            assertEquals(((Property) vp.properties("startTime").next()).key(), ((Property) detached.properties("startTime").next()).key());
            assertEquals(vp.values("endTime").next(), detached.values("endTime").next());
            assertEquals(((Property) vp.properties("endTime").next()).key(), ((Property) detached.properties("endTime").next()).key());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializePath() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().version(GraphSONVersion.V3_0).create().createMapper();
            final Path p = g.V(convertToVertexId("marko")).as("a").outE().as("b").inV().as("c").path()
                    .filter(t -> ((Vertex) t.get().objects().get(2)).value("name").equals("lop")).next();
            final String json = mapper.writeValueAsString(p);
            final Path detached = mapper.readValue(json, Path.class);

            assertNotNull(detached);
            assertEquals(p.labels().size(), detached.labels().size());
            assertEquals(p.labels().get(0).size(), detached.labels().get(0).size());
            assertEquals(p.labels().get(1).size(), detached.labels().get(1).size());
            assertEquals(p.labels().get(2).size(), detached.labels().get(2).size());
            assertTrue(p.labels().stream().flatMap(Collection::stream).allMatch(detached::hasLabel));

            final Vertex vOut = p.get("a");
            final Vertex detachedVOut = detached.get("a");
            assertEquals(vOut.label(), detachedVOut.label());
            assertEquals(vOut.id(), detachedVOut.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedVOut.properties().hasNext());

            final Edge e = p.get("b");
            final Edge detachedE = detached.get("b");
            assertEquals(e.label(), detachedE.label());
            assertEquals(e.id(), detachedE.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedE.properties().hasNext());

            final Vertex vIn = p.get("c");
            final Vertex detachedVIn = detached.get("c");
            assertEquals(vIn.label(), detachedVIn.label());
            assertEquals(vIn.id(), detachedVIn.id());

            // this is a SimpleTraverser so no properties are present in detachment
            assertFalse(detachedVIn.properties().hasNext());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeTraversalMetrics() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().version(GraphSONVersion.V3_0).create().createMapper();
            final TraversalMetrics before = g.V().both().profile().next();
            final String json = mapper.writeValueAsString(before);
            final TraversalMetrics after = mapper.readValue(json, TraversalMetrics.class);

            assertNotNull(after);
            assertEquals(before.getMetrics().size(), after.getMetrics().size());
            assertEquals(before.getDuration(TimeUnit.MILLISECONDS), after.getDuration(TimeUnit.MILLISECONDS));
            assertEquals(before.getMetrics().size(), after.getMetrics().size());

            before.getMetrics().forEach(b -> {
                final Optional<? extends Metrics> mFromA = after.getMetrics().stream().filter(a -> b.getId().equals(a.getId())).findFirst();
                if (mFromA.isPresent()) {
                    final Metrics m = mFromA.get();
                    assertEquals(b.getAnnotations(), m.getAnnotations());
                    assertEquals(b.getCounts(), m.getCounts());
                    assertEquals(b.getName(), m.getName());
                    assertEquals(b.getDuration(TimeUnit.MILLISECONDS), m.getDuration(TimeUnit.MILLISECONDS));
                } else {
                    fail("Metrics were not present after deserialization");
                }
            });
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        @org.junit.Ignore("TINKERPOP-1509")
        public void shouldSerializeTree() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().version(GraphSONVersion.V3_0).create().createMapper();
            final Tree t = g.V(convertToVertexId("marko")).out().properties("name").tree().next();
            final String json = mapper.writeValueAsString(t);

            final HashMap<String, Object> m = (HashMap<String, Object>) mapper.readValue(json, mapTypeReference);

            // Check Structure
            assertEquals(1, m.size());
            assertTrue(m.containsKey(convertToVertexId("marko").toString()));

            // Check Structure n+1
            final HashMap<String, Object> branch = (HashMap<String, Object>) m.get(convertToVertexId("marko").toString());
            assertEquals(2, branch.size());
            assertTrue(branch.containsKey(GraphSONTokens.KEY));
            assertTrue(branch.containsKey(GraphSONTokens.VALUE));

            //Check n+1 key (traversed element)
            final HashMap<String, Object> branchKey = (HashMap<String, Object>) branch.get(GraphSONTokens.KEY);
            assertTrue(branchKey.containsKey(GraphSONTokens.ID));
            assertTrue(branchKey.containsKey(GraphSONTokens.LABEL));
            assertTrue(branchKey.containsKey(GraphSONTokens.TYPE));
            assertTrue(branchKey.containsKey(GraphSONTokens.PROPERTIES));
            assertEquals(convertToVertexId("marko").toString(), branchKey.get(GraphSONTokens.ID).toString());
            assertEquals("person", branchKey.get(GraphSONTokens.LABEL));
            assertEquals("vertex", branchKey.get(GraphSONTokens.TYPE));
            final HashMap<String, List<HashMap<String, Object>>> branchKeyProps = (HashMap<String, List<HashMap<String, Object>>>) branchKey.get(GraphSONTokens.PROPERTIES);
            assertEquals("marko", branchKeyProps.get("name").get(0).get("value"));
            assertEquals(29, branchKeyProps.get("age").get(0).get("value"));

            //Check n+1 value (traversed element)
            final HashMap<String, Object> branchValue = (HashMap<String, Object>) branch.get(GraphSONTokens.VALUE);
            assertEquals(3, branchValue.size());
            assertTrue(branchValue.containsKey(convertToVertexId("vadas").toString()));
            assertTrue(branchValue.containsKey(convertToVertexId("lop").toString()));
            assertTrue(branchValue.containsKey(convertToVertexId("josh").toString()));

            // Check that vp[] functioned properly
            final HashMap<String, HashMap<String, Object>> branch2 = (HashMap<String, HashMap<String, Object>>) branchValue.get(convertToVertexId("vadas").toString());
            assertTrue(branch2.containsKey(GraphSONTokens.KEY));
            assertTrue(branch2.containsKey(GraphSONTokens.VALUE));

            final Map.Entry entry = branch2.get(GraphSONTokens.VALUE).entrySet().iterator().next();
            final HashMap<String, HashMap<String, Object>> branch2Prop = (HashMap<String, HashMap<String, Object>>) entry.getValue();
            assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.ID));
            assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.VALUE));
            assertTrue(branch2Prop.get(GraphSONTokens.KEY).containsKey(GraphSONTokens.LABEL));
            assertEquals("name", branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.LABEL));
            assertEquals("vadas", branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.VALUE));
            assertEquals(entry.getKey().toString(), branch2Prop.get(GraphSONTokens.KEY).get(GraphSONTokens.ID).toString());
        }
    }
}
