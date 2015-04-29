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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Serialization tests that occur at a lower level than IO.  Note that there is no need to test GraphML here as
 * it is not a format that can be used for generalized serialization (i.e. it only serializes an entire graph).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class SerializationTest {
    public static class KryoTest extends AbstractGremlinTest {
        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexAsDetached() throws Exception {
            final GryoIo gryoIo = graph.io(GryoIo.build());
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
            final GryoIo gryoIo = graph.io(GryoIo.build());
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
            final GryoIo gryoIo = graph.io(GryoIo.build());
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
            final GryoIo gryoIo = graph.io(GryoIo.build());
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
            final GryoIo gryoIo = graph.io(GryoIo.build());
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final VertexProperty<?> vertexProperty = graph.vertices(convertToVertexId("marko")).next().properties("location").next();
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
            final GryoIo gryoIo = graph.io(GryoIo.build());
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
            final GryoIo gryoIo = graph.io(GryoIo.build());
            final GryoWriter gryoWriter = gryoIo.writer().create();
            final GryoReader gryoReader = gryoIo.reader().create();

            final TraversalMetrics before = (TraversalMetrics) g.V().both().profile().cap(TraversalMetrics.METRICS_KEY).next();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            gryoWriter.writeObject(outputStream, before);

            final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            final TraversalMetrics after = gryoReader.readObject(inputStream, TraversalMetrics.class);
            assertNotNull(after);
            assertEquals(before.getMetrics().size(), after.getMetrics().size());
            assertEquals(before.getDuration(TimeUnit.MILLISECONDS), after.getDuration(TimeUnit.MILLISECONDS));
            assertEquals(before.getMetrics(0).getCounts(), after.getMetrics(0).getCounts());
        }
    }

    public static class GraphSONTest extends AbstractGremlinTest {
        private final TypeReference<HashMap<String, Object>> mapTypeReference = new TypeReference<HashMap<String, Object>>() {
        };

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertex() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build()).mapper().create().createMapper();
            final Vertex v = graph.vertices(convertToVertexId("marko")).next();
            final String json = mapper.writeValueAsString(v);
            final Map<String, Object> m = mapper.readValue(json, mapTypeReference);

            assertEquals(GraphSONTokens.VERTEX, m.get(GraphSONTokens.TYPE));
            assertEquals(v.label(), m.get(GraphSONTokens.LABEL));
            assertNotNull(m.get(GraphSONTokens.ID));
            assertEquals(v.value("name").toString(), ((List<Map>) m.get(GraphSONTokens.PROPERTIES)).stream().filter(map -> map.get(GraphSONTokens.LABEL).equals("name")).findFirst().get().get(GraphSONTokens.VALUE).toString());
            assertEquals((Integer) v.value("age"), ((List<Map>) m.get(GraphSONTokens.PROPERTIES)).stream().filter(map -> map.get(GraphSONTokens.LABEL).equals("age")).findFirst().get().get(GraphSONTokens.VALUE));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeEdge() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build()).mapper().create().createMapper();
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
            final ObjectMapper mapper = graph.io(GraphSONIo.build()).mapper().create().createMapper();
            final Property p = g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("weight");
            final String json = mapper.writeValueAsString(p);
            final Map<String, Object> m = mapper.readValue(json, mapTypeReference);

            assertEquals(p.value(), m.get(GraphSONTokens.VALUE));
            assertEquals(p.key(), m.get(GraphSONTokens.KEY));
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeVertexProperty() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build()).mapper().create().createMapper();
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
            final ObjectMapper mapper = graph.io(GraphSONIo.build()).mapper().create().createMapper();
            final VertexProperty vp = graph.vertices(convertToVertexId("marko")).next().properties("location").next();
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
            final ObjectMapper mapper = graph.io(GraphSONIo.build()).mapper().create().createMapper();
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
            assertEquals("marko", ((List<Map>) ((Map) objects.get(0)).get(GraphSONTokens.PROPERTIES)).stream().filter(map -> map.get(GraphSONTokens.LABEL).equals("name")).findFirst().get().get(GraphSONTokens.VALUE).toString());
            assertEquals("created", ((Map) objects.get(1)).get(GraphSONTokens.LABEL));
            assertEquals("lop", ((List<Map>) ((Map) objects.get(2)).get(GraphSONTokens.PROPERTIES)).stream().filter(map -> map.get(GraphSONTokens.LABEL).equals("name")).findFirst().get().get(GraphSONTokens.VALUE).toString());
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldSerializeTraversalMetrics() throws Exception {
            final ObjectMapper mapper = graph.io(GraphSONIo.build()).mapper().create().createMapper();
            final TraversalMetrics tm = (TraversalMetrics) g.V().both().profile().cap(TraversalMetrics.METRICS_KEY).next();
            final String json = mapper.writeValueAsString(tm);
            final Map<String, Object> m = mapper.readValue(json, mapTypeReference);

            assertTrue(m.containsKey(GraphSONTokens.DURATION));
            assertTrue(m.containsKey(GraphSONTokens.METRICS));

            final List<Map<String, Object>> metrics = (List<Map<String, Object>>) m.get(GraphSONTokens.METRICS);
            assertEquals(3, metrics.size());

            final Map<String, Object> metrics0 = metrics.get(0);
            assertTrue(metrics0.containsKey(GraphSONTokens.ID));
            assertTrue(metrics0.containsKey(GraphSONTokens.NAME));
            assertTrue(metrics0.containsKey(GraphSONTokens.COUNTS));
            assertTrue(metrics0.containsKey(GraphSONTokens.DURATION));
        }
    }
}
