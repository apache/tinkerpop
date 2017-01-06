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
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
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
import static org.junit.Assert.fail;


public class TinkerGraphGraphSONSerializerV2d0Test {

    // As of TinkerPop 3.2.1 default for GraphSON 2.0 means types enabled.
    private final Mapper defaultMapperV2d0 = GraphSONMapper.build()
            .version(GraphSONVersion.V2_0)
            .addCustomModule(GraphSONXModuleV2d0.build().create(false))
            .addRegistry(TinkerIoRegistryV2d0.instance())
            .create();

    private final Mapper noTypesMapperV2d0 = GraphSONMapper.build()
            .version(GraphSONVersion.V2_0)
            .addCustomModule(GraphSONXModuleV2d0.build().create(false))
            .typeInfo(TypeInfo.NO_TYPES)
            .addRegistry(TinkerIoRegistryV2d0.instance())
            .create();

    /**
     * Checks that the graph has been fully ser/deser with types.
     */
    @Test
    public void shouldDeserializeGraphSONIntoTinkerGraphWithPartialTypes() throws IOException {
        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);
        final  TinkerGraph baseModern = TinkerFactory.createModern();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, baseModern);
            final String json = out.toString();
            final TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            IoTest.assertModernGraph(read, true, false);
        }
    }

    /**
     * Checks that the graph has been fully ser/deser without types.
     */
    @Test
    public void shouldDeserializeGraphSONIntoTinkerGraphWithoutTypes() throws IOException {
        final GraphWriter writer = getWriter(noTypesMapperV2d0);
        final GraphReader reader = getReader(noTypesMapperV2d0);
        final TinkerGraph baseModern = TinkerFactory.createModern();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, baseModern);
            final String json = out.toString();
            final TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            IoTest.assertModernGraph(read, true, false);
        }
    }

    /**
     * Thorough types verification for Vertex ids, Vertex props, Edge ids, Edge props
     */
    @Test
    public void shouldDeserializeGraphSONIntoTinkerGraphKeepingTypes() throws IOException {
        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);

        final Graph sampleGraph1 = TinkerFactory.createModern();
        final Vertex v1 = sampleGraph1.addVertex(T.id, 100, "name", "kevin", "theUUID", UUID.randomUUID());
        final Vertex v2 = sampleGraph1.addVertex(T.id, 101L, "name", "henri", "theUUID", UUID.randomUUID());
        v1.addEdge("hello", v2, T.id, 101L,
                "uuid", UUID.randomUUID());

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, sampleGraph1);
            final String json = out.toString();

            final TinkerGraph read = reader.readObject(new ByteArrayInputStream(json.getBytes()), TinkerGraph.class);
            assertTrue(approximateGraphsCheck(sampleGraph1, read));
        }
    }

    /**
     * Asserts the approximateGraphsChecks function fails when expected. Vertex ids.
     */
    @Test
    public void shouldLooseTypesWithGraphSONNoTypesForVertexIds() throws IOException {
        final GraphWriter writer = getWriter(noTypesMapperV2d0);
        final GraphReader reader = getReader(noTypesMapperV2d0);
        final Graph sampleGraph1 = TinkerFactory.createModern();
        sampleGraph1.addVertex(T.id, 100L, "name", "kevin");
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, sampleGraph1);
            final String json = out.toString();
            final TinkerGraph read = TinkerGraph.open();
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
        final GraphWriter writer = getWriter(noTypesMapperV2d0);
        final GraphReader reader = getReader(noTypesMapperV2d0);
        final Graph sampleGraph1 = TinkerFactory.createModern();

        sampleGraph1.addVertex(T.id, 100, "name", "kevin", "uuid", UUID.randomUUID());
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, sampleGraph1);
            final String json = out.toString();
            final TinkerGraph read = TinkerGraph.open();
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
        final GraphWriter writer = getWriter(noTypesMapperV2d0);
        final  GraphReader reader = getReader(noTypesMapperV2d0);
        final Graph sampleGraph1 = TinkerFactory.createModern();
        final  Vertex v1 = sampleGraph1.addVertex(T.id, 100, "name", "kevin");
        v1.addEdge("hello", sampleGraph1.traversal().V().has("name", "marko").next(), T.id, 101L);
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, sampleGraph1);
            final String json = out.toString();
            final TinkerGraph read = TinkerGraph.open();
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
        final GraphWriter writer = getWriter(noTypesMapperV2d0);
        final GraphReader reader = getReader(noTypesMapperV2d0);
        final Graph sampleGraph1 = TinkerFactory.createModern();

        final Vertex v1 = sampleGraph1.addVertex(T.id, 100, "name", "kevin");
        v1.addEdge("hello", sampleGraph1.traversal().V().has("name", "marko").next(), T.id, 101,
                "uuid", UUID.randomUUID());
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, sampleGraph1);
            final String json = out.toString();
            final TinkerGraph read = TinkerGraph.open();
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
        final TinkerGraph tg = TinkerGraph.open();

        final Vertex v = tg.addVertex("vertexTest");
        final UUID uuidProp = UUID.randomUUID();
        final Duration durationProp = Duration.ofHours(3);
        final Long longProp = 2L;
        final ByteBuffer byteBufferProp = ByteBuffer.wrap("testbb".getBytes());
        final InetAddress inetAddressProp = InetAddress.getByName("10.10.10.10");

        // One Java util type natively supported by Jackson
        v.property("uuid", uuidProp);
        // One custom time type added by the GraphSON module
        v.property("duration", durationProp);
        // One Java native type not handled by JSON natively
        v.property("long", longProp);
        // One Java util type added by GraphSON
        v.property("bytebuffer", byteBufferProp);
        v.property("inetaddress", inetAddressProp);


        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, tg);
            final String json = out.toString();
            final TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            final Vertex vRead = read.traversal().V().hasLabel("vertexTest").next();
            assertEquals(vRead.property("uuid").value(), uuidProp);
            assertEquals(vRead.property("duration").value(), durationProp);
            assertEquals(vRead.property("long").value(), longProp);
            assertEquals(vRead.property("bytebuffer").value(), byteBufferProp);
            assertEquals(vRead.property("inetaddress").value(), inetAddressProp);
        }
    }


    @Test
    public void deserializersTestsVertex() {
        final TinkerGraph tg = TinkerGraph.open();

        final Vertex v = tg.addVertex("vertexTest");
        v.property("born", LocalDateTime.of(1971, 1, 2, 20, 50));
        v.property("dead", LocalDateTime.of(1971, 1, 7, 20, 50));

        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, v);
            final String json = out.toString();

            // Object works, because there's a type in the payload now
            // Vertex.class would work as well
            // Anything else would not because we check the type in param here with what's in the JSON, for safety.
            final Vertex vRead = (Vertex)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            assertEquals(v, vRead);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    @Test
    public void deserializersTestsEdge() {
        final TinkerGraph tg = TinkerGraph.open();

        final Vertex v = tg.addVertex("vertexTest");
        final Vertex v2 = tg.addVertex("vertexTest");

        final Edge ed = v.addEdge("knows", v2, "time", LocalDateTime.now());

        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, ed);
            final String json = out.toString();

            // Object works, because there's a type in the payload now
            // Edge.class would work as well
            // Anything else would not because we check the type in param here with what's in the JSON, for safety.
            final Edge eRead = (Edge)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            assertEquals(ed, eRead);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    @Test
    public void deserializersTestsTinkerGraph() {
        final TinkerGraph tg = TinkerGraph.open();

        final Vertex v = tg.addVertex("vertexTest");
        final Vertex v2 = tg.addVertex("vertexTest");

        v.addEdge("knows", v2);

        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, tg);
            final String json = out.toString();

            final Graph gRead = (Graph)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            assertTrue(approximateGraphsCheck(tg, gRead));
        } catch (IOException e) {
            e.printStackTrace();
            fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    @Test
    public void deserializersTestsProperty() {
        final TinkerGraph tg = TinkerGraph.open();

        final Vertex v = tg.addVertex("vertexTest");
        final Vertex v2 = tg.addVertex("vertexTest");

        final Edge ed = v.addEdge("knows", v2);

        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);

        final Property prop = ed.property("since", Year.parse("1993"));

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, prop);
            final String json = out.toString();

            final Property pRead = (Property)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            //can't use equals here, because pRead is detached, its parent element has not been intentionally
            //serialized and "equals()" checks that.
            assertTrue(prop.key().equals(pRead.key()) && prop.value().equals(pRead.value()));
        } catch (IOException e) {
            e.printStackTrace();
            fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    @Test
    public void deserializersTestsVertexProperty() {
        final TinkerGraph tg = TinkerGraph.open();

        final Vertex v = tg.addVertex("vertexTest");

        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);

        final VertexProperty prop = v.property("born", LocalDateTime.of(1971, 1, 2, 20, 50));

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, prop);
            final String json = out.toString();

            final VertexProperty vPropRead = (VertexProperty)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            //only classes and ids are checked, that's ok, full vertex property ser/de
            //is checked elsewhere.
            assertEquals(prop, vPropRead);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    @Test
    public void deserializersTestsPath() {
        final TinkerGraph tg = TinkerFactory.createModern();

        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);

        final Path p = tg.traversal().V(1).as("a").has("name").as("b").
                out("knows").out("created").as("c").
                has("name", "ripple").values("name").as("d").
                identity().as("e").path().next();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, p);
            final String json = out.toString();

            final Path pathRead = (Path)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);

            for (int i = 0; i < p.objects().size(); i++) {
                final Object o = p.objects().get(i);
                final Object oRead = pathRead.objects().get(i);
                assertEquals(o, oRead);
            }
            for (int i = 0; i < p.labels().size(); i++) {
                final Set<String> o = p.labels().get(i);
                final Set<String> oRead = pathRead.labels().get(i);
                assertEquals(o, oRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    @Test
    public void deserializersTestsMetrics() {
        final TinkerGraph tg = TinkerFactory.createModern();

        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);

        final TraversalMetrics tm = tg.traversal().V(1).as("a").has("name").as("b").
                out("knows").out("created").as("c").
                has("name", "ripple").values("name").as("d").
                identity().as("e").profile().next();

        final MutableMetrics m = new MutableMetrics(tm.getMetrics(0));
        // making sure nested metrics are included in serde
        m.addNested(new MutableMetrics(tm.getMetrics(1)));

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, m);
            final String json = out.toString();

            final Metrics metricsRead = (Metrics)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            // toString should be enough to compare Metrics
            assertTrue(m.toString().equals(metricsRead.toString()));
        } catch (IOException e) {
            e.printStackTrace();
            fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    @Test
    public void deserializersTestsTraversalMetrics() {
        final TinkerGraph tg = TinkerFactory.createModern();

        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);

        final TraversalMetrics tm = tg.traversal().V(1).as("a").has("name").as("b").
                out("knows").out("created").as("c").
                has("name", "ripple").values("name").as("d").
                identity().as("e").profile().next();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, tm);
            final String json = out.toString();

            final TraversalMetrics traversalMetricsRead = (TraversalMetrics)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            // toString should be enough to compare TraversalMetrics
            assertTrue(tm.toString().equals(traversalMetricsRead.toString()));
        } catch (IOException e) {
            e.printStackTrace();
            fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    @Test
    public void deserializersTestsTree() {
        final TinkerGraph tg = TinkerFactory.createModern();

        final GraphWriter writer = getWriter(defaultMapperV2d0);
        final GraphReader reader = getReader(defaultMapperV2d0);

        final Tree t = tg.traversal().V().out().out().tree().next();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, t);
            final String json = out.toString();

            Tree treeRead = (Tree)reader.readObject(new ByteArrayInputStream(json.getBytes()), Object.class);
            //Map's equals should check each component of the tree recursively
            //on each it will call "equals()" which for Vertices will compare ids, which
            //is ok. Complete vertex deser is checked elsewhere.
            assertEquals(t, treeRead);

        } catch (IOException e) {
            e.printStackTrace();
            fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    private GraphWriter getWriter(Mapper paramMapper) {
        return GraphSONWriter.build().mapper(paramMapper).create();
    }

    private GraphReader getReader(Mapper paramMapper) {
        return GraphSONReader.build().mapper(paramMapper).create();
    }

    /**
     * Checks sequentially vertices and edges of both graphs. Will check sequentially Vertex IDs, Vertex Properties IDs
     * and values and classes. Then same for edges. To use when serializing a Graph and deserializing the supposedly
     * same Graph.
     */
    private boolean approximateGraphsCheck(Graph g1, Graph g2) {
        final Iterator<Vertex> itV = g1.vertices();
        final Iterator<Vertex> itVRead = g2.vertices();

        while (itV.hasNext()) {
            final Vertex v = itV.next();
            final Vertex vRead = itVRead.next();

            // Will only check IDs but that's 'good' enough.
            if (!v.equals(vRead)) {
                return false;
            }

            final Iterator itVP = v.properties();
            final Iterator itVPRead = vRead.properties();
            while (itVP.hasNext()) {
                final VertexProperty vp = (VertexProperty) itVP.next();
                final VertexProperty vpRead = (VertexProperty) itVPRead.next();
                if (!vp.value().equals(vpRead.value())
                        || !vp.equals(vpRead)) {
                    return false;
                }
            }
        }

        final Iterator<Edge> itE = g1.edges();
        final Iterator<Edge> itERead = g2.edges();

        while (itE.hasNext()) {
            final Edge e = itE.next();
            final Edge eRead = itERead.next();
            // Will only check IDs but that's good enough.
            if (!e.equals(eRead)) {
                return false;
            }

            final Iterator itEP = e.properties();
            final Iterator itEPRead = eRead.properties();
            while (itEP.hasNext()) {
                final Property ep = (Property) itEP.next();
                final Property epRead = (Property) itEPRead.next();
                if (!ep.value().equals(epRead.value())
                        || !ep.equals(epRead)) {
                    return false;
                }
            }
        }
        return true;
    }
}
