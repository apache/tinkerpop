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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Year;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TinkerGraphGraphSONSerializerV2d0Test {

    // As of TinkerPop 3.2.1 default for GraphSON 2.0 means types enabled.
    Mapper defaultMapperV2d0 = GraphSONMapper.build()
            .version(GraphSONVersion.V2_0)
            .addRegistry(TinkerIoRegistryV2d0.getInstance())
            .create();

    Mapper noTypesMapperV2d0 = GraphSONMapper.build()
            .version(GraphSONVersion.V2_0)
            .typeInfo(TypeInfo.NO_TYPES)
            .addRegistry(TinkerIoRegistryV2d0.getInstance())
            .create();

    /**
     * Checks that the graph has been fully ser/deser with types.
     */
    @Test
    public void shouldDeserializeGraphSONIntoTinkerGraphWithPartialTypes() throws IOException {
        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);
        TinkerGraph baseModern = TinkerFactory.createModern();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, baseModern);
            String json = out.toString();
            TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            IoTest.assertModernGraph(read, true, false);
        }
    }

    /**
     * Checks that the graph has been fully ser/deser without types.
     */
    @Test
    public void shouldDeserializeGraphSONIntoTinkerGraphWithoutTypes() throws IOException {
        GraphWriter writer = getWriter(noTypesMapperV2d0);
        GraphReader reader = getReader(noTypesMapperV2d0);
        TinkerGraph baseModern = TinkerFactory.createModern();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, baseModern);
            String json = out.toString();
            TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            IoTest.assertModernGraph(read, true, false);
        }
    }

    /**
     * Thorough types verification for Vertex ids, Vertex props, Edge ids, Edge props
     */
    @Test
    public void shouldDeserializeGraphSONIntoTinkerGraphKeepingTypes() throws IOException {
        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);

        Graph sampleGraph1 = TinkerFactory.createModern();
        Vertex v1 = sampleGraph1.addVertex(T.id, 100, "name", "kevin", "theUUID", UUID.randomUUID());
        Vertex v2 = sampleGraph1.addVertex(T.id, 101L, "name", "henri", "theUUID", UUID.randomUUID());
        v1.addEdge("hello", v2, T.id, 101L,
                "uuid", UUID.randomUUID());

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, sampleGraph1);
            String json = out.toString();

            TinkerGraph read = reader.readObject(new ByteArrayInputStream(json.getBytes()), TinkerGraph.class);
            assertTrue(approximateGraphsCheck(sampleGraph1, read));
        }
    }

    /**
     * Asserts the approximateGraphsChecks function fails when expected. Vertex ids.
     */
    @Test
    public void shouldLooseTypesWithGraphSONNoTypesForVertexIds() throws IOException {
        GraphWriter writer = getWriter(noTypesMapperV2d0);
        GraphReader reader = getReader(noTypesMapperV2d0);
        Graph sampleGraph1 = TinkerFactory.createModern();
        sampleGraph1.addVertex(T.id, 100L, "name", "kevin");
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, sampleGraph1);
            String json = out.toString();
            TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            // Should fail on deserialized vertex Id.
            assertFalse(approximateGraphsCheck(sampleGraph1, read));
        }
    }

    /**
     * Asserts the approximateGraphsChecks function fails when expected. Vertex props.
     */
    @Test
    public void shouldLooseTypesWithGraphSONNoTypesForVertexProps() throws IOException {
        GraphWriter writer = getWriter(noTypesMapperV2d0);
        GraphReader reader = getReader(noTypesMapperV2d0);
        Graph sampleGraph1 = TinkerFactory.createModern();

        sampleGraph1.addVertex(T.id, 100, "name", "kevin", "uuid", UUID.randomUUID());
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, sampleGraph1);
            String json = out.toString();
            TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            // Should fail on deserialized vertex prop.
            assertFalse(approximateGraphsCheck(sampleGraph1, read));
        }
    }

    /**
     * Asserts the approximateGraphsChecks function fails when expected. Edge ids.
     */
    @Test
    public void shouldLooseTypesWithGraphSONNoTypesForEdgeIds() throws IOException {
        GraphWriter writer = getWriter(noTypesMapperV2d0);
        GraphReader reader = getReader(noTypesMapperV2d0);
        Graph sampleGraph1 = TinkerFactory.createModern();
        Vertex v1 = sampleGraph1.addVertex(T.id, 100, "name", "kevin");
        v1.addEdge("hello", sampleGraph1.traversal().V().has("name", "marko").next(), T.id, 101L);
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, sampleGraph1);
            String json = out.toString();
            TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            // Should fail on deserialized edge Id.
            assertFalse(approximateGraphsCheck(sampleGraph1, read));
        }
    }

    /**
     * Asserts the approximateGraphsChecks function fails when expected. Edge props.
     */
    @Test
    public void shouldLooseTypesWithGraphSONNoTypesForEdgeProps() throws IOException {
        GraphWriter writer = getWriter(noTypesMapperV2d0);
        GraphReader reader = getReader(noTypesMapperV2d0);
        Graph sampleGraph1 = TinkerFactory.createModern();

        Vertex v1 = sampleGraph1.addVertex(T.id, 100, "name", "kevin");
        v1.addEdge("hello", sampleGraph1.traversal().V().has("name", "marko").next(), T.id, 101,
                "uuid", UUID.randomUUID());
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, sampleGraph1);
            String json = out.toString();
            TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            // Should fail on deserialized edge prop.
            assertFalse(approximateGraphsCheck(sampleGraph1, read));
        }
    }

    /**
     * Those kinds of types are declared differently in the GraphSON type deserializer, check that all are handled
     * properly.
     */
    @Test
    public void shouldKeepTypesWhenDeserializingSerializedTinkerGraph() throws IOException {
        TinkerGraph tg = TinkerGraph.open();

        Vertex v = tg.addVertex("vertexTest");
        UUID uuidProp = UUID.randomUUID();
        Duration durationProp = Duration.ofHours(3);
        Long longProp = 2L;
        ByteBuffer byteBufferProp = ByteBuffer.wrap("testbb".getBytes());

        // One Java util type natively supported by Jackson
        v.property("uuid", uuidProp);
        // One custom time type added by the GraphSON module
        v.property("duration", durationProp);
        // One Java native type not handled by JSON natively
        v.property("long", longProp);
        // One Java util type added by GraphSON
        v.property("bytebuffer", byteBufferProp);

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, tg);
            String json = out.toString();
            TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            Vertex vRead = read.traversal().V().hasLabel("vertexTest").next();
            assertEquals(vRead.property("uuid").value(), uuidProp);
            assertEquals(vRead.property("duration").value(), durationProp);
            assertEquals(vRead.property("long").value(), longProp);
            assertEquals(vRead.property("bytebuffer").value(), byteBufferProp);
        }
    }


    @Test
    public void deserializersTestsVertex() {
        TinkerGraph tg = TinkerGraph.open();

        Vertex v = tg.addVertex("vertexTest");
        v.property("born", LocalDateTime.of(1971, 1, 2, 20, 50));
        v.property("dead", LocalDateTime.of(1971, 1, 7, 20, 50));

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, v);
            String json = out.toString();

            // Object works, because there's a type in the payload now
            // Vertex.class would work as well
            // Anything else would not because we check the type in param here with what's in the JSON, for safety.
            Vertex vRead = (Vertex)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            assertEquals(v, vRead);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deserializersTestsEdge() {
        TinkerGraph tg = TinkerGraph.open();

        Vertex v = tg.addVertex("vertexTest");
        Vertex v2 = tg.addVertex("vertexTest");

        Edge ed = v.addEdge("knows", v2, "time", LocalDateTime.now());

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, ed);
            String json = out.toString();

            // Object works, because there's a type in the payload now
            // Edge.class would work as well
            // Anything else would not because we check the type in param here with what's in the JSON, for safety.
            Edge eRead = (Edge)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            assertEquals(ed, eRead);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deserializersTestsTinkerGraph() {
        TinkerGraph tg = TinkerGraph.open();

        Vertex v = tg.addVertex("vertexTest");
        Vertex v2 = tg.addVertex("vertexTest");

        Edge ed = v.addEdge("knows", v2);

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, tg);
            String json = out.toString();

            Graph gRead = (Graph)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            assertTrue(approximateGraphsCheck(tg, gRead));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deserializersTestsProperty() {
        TinkerGraph tg = TinkerGraph.open();

        Vertex v = tg.addVertex("vertexTest");
        Vertex v2 = tg.addVertex("vertexTest");

        Edge ed = v.addEdge("knows", v2);

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);

        Property prop = ed.property("since", Year.parse("1993"));

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, prop);
            String json = out.toString();

            Property pRead = (Property)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            //can't use equals here, because pRead is detached, its parent element has not been intentionally
            //serialized and "equals()" checks that.
            assertTrue(prop.key().equals(pRead.key()) && prop.value().equals(pRead.value()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deserializersTestsVertexProperty() {
        TinkerGraph tg = TinkerGraph.open();

        Vertex v = tg.addVertex("vertexTest");

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);

        VertexProperty prop = v.property("born", LocalDateTime.of(1971, 1, 2, 20, 50));

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, prop);
            String json = out.toString();

            VertexProperty vPropRead = (VertexProperty)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            //only classes and ids are checked, that's ok, full vertex property ser/de
            //is checked elsewhere.
            assertEquals(prop, vPropRead);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deserializersTestsPath() {
        TinkerGraph tg = TinkerFactory.createModern();

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);

        Path p = tg.traversal().V(1).as("a").has("name").as("b").
                out("knows").out("created").as("c").
                has("name", "ripple").values("name").as("d").
                identity().as("e").path().next();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, p);
            String json = out.toString();

            Path pathRead = (Path)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);

            for (int i = 0; i < p.objects().size(); i++) {
                Object o = p.objects().get(i);
                Object oRead = pathRead.objects().get(i);
                assertEquals(o, oRead);
            }
            for (int i = 0; i < p.labels().size(); i++) {
                Set<String> o = p.labels().get(i);
                Set<String> oRead = pathRead.labels().get(i);
                assertEquals(o, oRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deserializersTestsMetrics() {
        TinkerGraph tg = TinkerFactory.createModern();

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);

        TraversalMetrics tm = tg.traversal().V(1).as("a").has("name").as("b").
                out("knows").out("created").as("c").
                has("name", "ripple").values("name").as("d").
                identity().as("e").profile().next();

        MutableMetrics m = new MutableMetrics(tm.getMetrics(0));
        // making sure nested metrics are included in serde
        m.addNested(new MutableMetrics(tm.getMetrics(1)));

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, m);
            String json = out.toString();

            Metrics metricsRead = (Metrics)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            // toString should be enough to compare Metrics
            assertTrue(m.toString().equals(metricsRead.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deserializersTestsTraversalMetrics() {
        TinkerGraph tg = TinkerFactory.createModern();

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);

        TraversalMetrics tm = tg.traversal().V(1).as("a").has("name").as("b").
                out("knows").out("created").as("c").
                has("name", "ripple").values("name").as("d").
                identity().as("e").profile().next();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, tm);
            String json = out.toString();

            TraversalMetrics traversalMetricsRead = (TraversalMetrics)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            // toString should be enough to compare TraversalMetrics
            assertTrue(tm.toString().equals(traversalMetricsRead.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deserializersTree() {
        TinkerGraph tg = TinkerFactory.createModern();

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);

        Tree t = tg.traversal().V().out().out().tree().next();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, t);
            String json = out.toString();

            Tree treeRead = (Tree)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            //Map's equals should check each component of the tree recursively
            //on each it will call "equals()" which for Vertices will compare ids, which
            //is ok. Complete vertex deser is checked elsewhere.
            assertEquals(t, treeRead);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private GraphWriter getWriter(Mapper paramMapper) {
        return GraphSONWriter.build().mapper(paramMapper).create();
    }

    private GraphReader getReader(Mapper paramMapper) {
        return GraphSONReader.build().mapper(paramMapper).create();
    }

    /**
     * Checks sequentially vertices and egdes of both graphs. Will check sequentially Vertex IDs, Vertex Properties IDs
     * and values and classes. Then same for edges. To use when serializing a Graph and deserializing the supposedly
     * same Graph.
     */
    private boolean approximateGraphsCheck(Graph g1, Graph g2) {
        Iterator<Vertex> itV = g1.vertices();
        Iterator<Vertex> itVRead = g2.vertices();

        while (itV.hasNext()) {
            Vertex v = itV.next();
            Vertex vRead = itVRead.next();

            // Will only check IDs but that's 'good' enough.
            if (!v.equals(vRead)) {
                return false;
            }

            Iterator itVP = v.properties();
            Iterator itVPRead = vRead.properties();
            while (itVP.hasNext()) {
                VertexProperty vp = (VertexProperty) itVP.next();
                VertexProperty vpRead = (VertexProperty) itVPRead.next();
                if (!vp.value().equals(vpRead.value())
                        || !vp.equals(vpRead)) {
                    return false;
                }
            }
        }

        Iterator<Edge> itE = g1.edges();
        Iterator<Edge> itERead = g2.edges();

        while (itE.hasNext()) {
            Edge e = itE.next();
            Edge eRead = itERead.next();
            // Will only check IDs but that's good enough.
            if (!e.equals(eRead)) {
                return false;
            }

            Iterator itEP = e.properties();
            Iterator itEPRead = eRead.properties();
            while (itEP.hasNext()) {
                Property ep = (Property) itEP.next();
                Property epRead = (Property) itEPRead.next();
                if (!ep.value().equals(epRead.value())
                        || !ep.equals(epRead)) {
                    return false;
                }
            }
        }
        return true;
    }
}
