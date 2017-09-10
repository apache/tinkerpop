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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoClassResolver;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.kryo.ClassResolver;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Registration;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.junit.Test;

import java.awt.Color;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TinkerGraphTest {

    @Test
    public void shouldManageIndices() {
        final TinkerGraph g = TinkerGraph.open();

        Set<String> keys = g.getIndexedKeys(Vertex.class);
        assertEquals(0, keys.size());
        keys = g.getIndexedKeys(Edge.class);
        assertEquals(0, keys.size());

        g.createIndex("name1", Vertex.class);
        g.createIndex("name2", Vertex.class);
        g.createIndex("oid1", Edge.class);
        g.createIndex("oid2", Edge.class);

        // add the same one twice to check idempotance
        g.createIndex("name1", Vertex.class);

        keys = g.getIndexedKeys(Vertex.class);
        assertEquals(2, keys.size());
        for (String k : keys) {
            assertTrue(k.equals("name1") || k.equals("name2"));
        }

        keys = g.getIndexedKeys(Edge.class);
        assertEquals(2, keys.size());
        for (String k : keys) {
            assertTrue(k.equals("oid1") || k.equals("oid2"));
        }

        g.dropIndex("name2", Vertex.class);
        keys = g.getIndexedKeys(Vertex.class);
        assertEquals(1, keys.size());
        assertEquals("name1", keys.iterator().next());

        g.dropIndex("name1", Vertex.class);
        keys = g.getIndexedKeys(Vertex.class);
        assertEquals(0, keys.size());

        g.dropIndex("oid1", Edge.class);
        keys = g.getIndexedKeys(Edge.class);
        assertEquals(1, keys.size());
        assertEquals("oid2", keys.iterator().next());

        g.dropIndex("oid2", Edge.class);
        keys = g.getIndexedKeys(Edge.class);
        assertEquals(0, keys.size());

        g.dropIndex("better-not-error-index-key-does-not-exist", Vertex.class);
        g.dropIndex("better-not-error-index-key-does-not-exist", Edge.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotCreateVertexIndexWithNullKey() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex(null, Vertex.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotCreateEdgeIndexWithNullKey() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex(null, Edge.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotCreateVertexIndexWithEmptyKey() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("", Vertex.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotCreateEdgeIndexWithEmptyKey() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("", Edge.class);
    }

    @Test
    public void shouldUpdateVertexIndicesInNewGraph() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("name", Vertex.class);

        g.addVertex("name", "marko", "age", 29);
        g.addVertex("name", "stephen", "age", 35);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is used because only "stephen" ages should pass through the pipeline due to the inclusion of the
        // key index lookup on "name".  If there's an age of something other than 35 in the pipeline being evaluated
        // then something is wrong.
        assertEquals(new Long(1), g.traversal().V().has("age", P.test((t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35)).has("name", "stephen").count().next());
    }

    @Test
    public void shouldRemoveAVertexFromAnIndex() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("name", Vertex.class);

        g.addVertex("name", "marko", "age", 29);
        g.addVertex("name", "stephen", "age", 35);
        final Vertex v = g.addVertex("name", "stephen", "age", 35);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is used because only "stephen" ages should pass through the pipeline due to the inclusion of the
        // key index lookup on "name".  If there's an age of something other than 35 in the pipeline being evaluated
        // then something is wrong.
        assertEquals(new Long(2), g.traversal().V().has("age", P.test((t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35)).has("name", "stephen").count().next());

        v.remove();
        assertEquals(new Long(1), g.traversal().V().has("age", P.test((t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35)).has("name", "stephen").count().next());
    }

    @Test
    public void shouldUpdateVertexIndicesInExistingGraph() {
        final TinkerGraph g = TinkerGraph.open();

        g.addVertex("name", "marko", "age", 29);
        g.addVertex("name", "stephen", "age", 35);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is not used because "stephen" and "marko" ages both pass through the pipeline.
        assertEquals(new Long(1), g.traversal().V().has("age", P.test((t, u) -> {
            assertTrue(t.equals(35) || t.equals(29));
            return true;
        }, 35)).has("name", "stephen").count().next());

        g.createIndex("name", Vertex.class);

        // another spy into the pipeline for index check.  in this case, we know that at index
        // is used because only "stephen" ages should pass through the pipeline due to the inclusion of the
        // key index lookup on "name".  If there's an age of something other than 35 in the pipeline being evaluated
        // then something is wrong.
        assertEquals(new Long(1), g.traversal().V().has("age", P.test((t, u) -> {
            assertEquals(35, t);
            return true;
        }, 35)).has("name", "stephen").count().next());
    }

    @Test
    public void shouldUpdateEdgeIndicesInNewGraph() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("oid", Edge.class);

        final Vertex v = g.addVertex();
        v.addEdge("friend", v, "oid", "1", "weight", 0.5f);
        v.addEdge("friend", v, "oid", "2", "weight", 0.6f);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is used because only oid 1 should pass through the pipeline due to the inclusion of the
        // key index lookup on "oid".  If there's an weight of something other than 0.5f in the pipeline being
        // evaluated then something is wrong.
        assertEquals(new Long(1), g.traversal().E().has("weight", P.test((t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5)).has("oid", "1").count().next());
    }

    @Test
    public void shouldRemoveEdgeFromAnIndex() {
        final TinkerGraph g = TinkerGraph.open();
        g.createIndex("oid", Edge.class);

        final Vertex v = g.addVertex();
        v.addEdge("friend", v, "oid", "1", "weight", 0.5f);
        final Edge e = v.addEdge("friend", v, "oid", "1", "weight", 0.5f);
        v.addEdge("friend", v, "oid", "2", "weight", 0.6f);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is used because only oid 1 should pass through the pipeline due to the inclusion of the
        // key index lookup on "oid".  If there's an weight of something other than 0.5f in the pipeline being
        // evaluated then something is wrong.
        assertEquals(new Long(2), g.traversal().E().has("weight", P.test((t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5)).has("oid", "1").count().next());

        e.remove();
        assertEquals(new Long(1), g.traversal().E().has("weight", P.test((t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5)).has("oid", "1").count().next());
    }

    @Test
    public void shouldUpdateEdgeIndicesInExistingGraph() {
        final TinkerGraph g = TinkerGraph.open();

        final Vertex v = g.addVertex();
        v.addEdge("friend", v, "oid", "1", "weight", 0.5f);
        v.addEdge("friend", v, "oid", "2", "weight", 0.6f);

        // a tricky way to evaluate if indices are actually being used is to pass a fake BiPredicate to has()
        // to get into the Pipeline and evaluate what's going through it.  in this case, we know that at index
        // is not used because "1" and "2" weights both pass through the pipeline.
        assertEquals(new Long(1), g.traversal().E().has("weight", P.test((t, u) -> {
            assertTrue(t.equals(0.5f) || t.equals(0.6f));
            return true;
        }, 0.5)).has("oid", "1").count().next());

        g.createIndex("oid", Edge.class);

        // another spy into the pipeline for index check.  in this case, we know that at index
        // is used because only oid 1 should pass through the pipeline due to the inclusion of the
        // key index lookup on "oid".  If there's an weight of something other than 0.5f in the pipeline being
        // evaluated then something is wrong.
        assertEquals(new Long(1), g.traversal().E().has("weight", P.test((t, u) -> {
            assertEquals(0.5f, t);
            return true;
        }, 0.5)).has("oid", "1").count().next());
    }

    @Test
    public void shouldSerializeTinkerGraphToGryo() throws Exception {
        final TinkerGraph graph = TinkerFactory.createModern();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            graph.io(IoCore.gryo()).writer().create().writeObject(out, graph);
            final byte[] b = out.toByteArray();
            try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(b)) {
                final TinkerGraph target = graph.io(IoCore.gryo()).reader().create().readObject(inputStream, TinkerGraph.class);
                IoTest.assertModernGraph(target, true, false);
            }
        }
    }

    @Test
    public void shouldSerializeTinkerGraphWithMultiPropertiesToGryo() throws Exception {
        final TinkerGraph graph = TinkerFactory.createTheCrew();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            graph.io(IoCore.gryo()).writer().create().writeObject(out, graph);
            final byte[] b = out.toByteArray();
            try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(b)) {
                final TinkerGraph target = graph.io(IoCore.gryo()).reader().create().readObject(inputStream, TinkerGraph.class);
                IoTest.assertCrewGraph(target, false);
            }
        }
    }

    @Test
    public void shouldSerializeTinkerGraphToGraphSON() throws Exception {
        final TinkerGraph graph = TinkerFactory.createModern();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            graph.io(IoCore.graphson()).writer().create().writeObject(out, graph);
            try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(out.toByteArray())) {
                final TinkerGraph target = graph.io(IoCore.graphson()).reader().create().readObject(inputStream, TinkerGraph.class);
                IoTest.assertModernGraph(target, true, false);
            }
        }
    }

    @Test
    public void shouldSerializeTinkerGraphWithMultiPropertiesToGraphSON() throws Exception {
        final TinkerGraph graph = TinkerFactory.createTheCrew();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            graph.io(IoCore.graphson()).writer().create().writeObject(out, graph);
            try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(out.toByteArray())) {
                final TinkerGraph target = graph.io(IoCore.graphson()).reader().create().readObject(inputStream, TinkerGraph.class);
                IoTest.assertCrewGraph(target, false);
            }
        }
    }

    @Test
    public void shouldSerializeTinkerGraphToGraphSONWithTypes() throws Exception {
        final TinkerGraph graph = TinkerFactory.createModern();
        final Mapper<ObjectMapper> mapper = graph.io(IoCore.graphson()).mapper().embedTypes(true).create();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final GraphWriter writer = GraphSONWriter.build().mapper(mapper).create();
            writer.writeObject(out, graph);
            try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(out.toByteArray())) {
                final GraphReader reader = GraphSONReader.build().mapper(mapper).create();
                final TinkerGraph target = reader.readObject(inputStream, TinkerGraph.class);
                IoTest.assertModernGraph(target, true, false);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRequireGraphLocationIfFormatIsSet() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "graphml");
        TinkerGraph.open(conf);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotModifyAVertexThatWasRemoved() {
        final TinkerGraph graph = TinkerGraph.open();
        final Vertex v = graph.addVertex();
        v.property("name", "stephen");

        assertEquals("stephen", v.value("name"));
        v.remove();

        v.property("status", 1);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotAddEdgeToAVertexThatWasRemoved() {
        final TinkerGraph graph = TinkerGraph.open();
        final Vertex v = graph.addVertex();
        v.property("name", "stephen");

        assertEquals("stephen", v.value("name"));
        v.remove();
        v.addEdge("self", v);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotReadValueOfPropertyOnVertexThatWasRemoved() {
        final TinkerGraph graph = TinkerGraph.open();
        final Vertex v = graph.addVertex();
        v.property("name", "stephen");

        assertEquals("stephen", v.value("name"));
        v.remove();
        v.value("name");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRequireGraphFormatIfLocationIsSet() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, TestHelper.makeTestDataDirectory(TinkerGraphTest.class));
        TinkerGraph.open(conf);
    }

    @Test
    public void shouldPersistToGraphML() {
        final String graphLocation = TestHelper.makeTestDataDirectory(TinkerGraphTest.class) + "shouldPersistToGraphML.xml";
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) f.delete();

        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "graphml");
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, graphLocation);
        final TinkerGraph graph = TinkerGraph.open(conf);
        TinkerFactory.generateModern(graph);
        graph.close();

        final TinkerGraph reloadedGraph = TinkerGraph.open(conf);
        IoTest.assertModernGraph(reloadedGraph, true, true);
        reloadedGraph.close();
    }

    @Test
    public void shouldPersistToGraphSON() {
        final String graphLocation = TestHelper.makeTestDataDirectory(TinkerGraphTest.class) + "shouldPersistToGraphSON.json";
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) f.delete();

        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "graphson");
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, graphLocation);
        final TinkerGraph graph = TinkerGraph.open(conf);
        TinkerFactory.generateModern(graph);
        graph.close();

        final TinkerGraph reloadedGraph = TinkerGraph.open(conf);
        IoTest.assertModernGraph(reloadedGraph, true, false);
        reloadedGraph.close();
    }

    @Test
    public void shouldPersistToGryo() {
        final String graphLocation = TestHelper.makeTestDataDirectory(TinkerGraphTest.class) + "shouldPersistToGryo.kryo";
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) f.delete();

        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, graphLocation);
        final TinkerGraph graph = TinkerGraph.open(conf);
        TinkerFactory.generateModern(graph);
        graph.close();

        final TinkerGraph reloadedGraph = TinkerGraph.open(conf);
        IoTest.assertModernGraph(reloadedGraph, true, false);
        reloadedGraph.close();
    }

    @Test
    public void shouldPersistToGryoAndHandleMultiProperties() {
        final String graphLocation = TestHelper.makeTestDataDirectory(TinkerGraphTest.class) + "shouldPersistToGryoMulti.kryo";
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) f.delete();

        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, graphLocation);
        final TinkerGraph graph = TinkerGraph.open(conf);
        TinkerFactory.generateTheCrew(graph);
        graph.close();

        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.toString());
        final TinkerGraph reloadedGraph = TinkerGraph.open(conf);
        IoTest.assertCrewGraph(reloadedGraph, false);
        reloadedGraph.close();
    }

    @Test
    public void shouldPersistWithRelativePath() {
        final String graphLocation = TestHelper.convertToRelative(TinkerGraphTest.class,
                new File(TestHelper.makeTestDataDirectory(TinkerGraphTest.class)))  + "shouldPersistToGryoRelative.kryo";
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) f.delete();

        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, graphLocation);
        final TinkerGraph graph = TinkerGraph.open(conf);
        TinkerFactory.generateModern(graph);
        graph.close();

        final TinkerGraph reloadedGraph = TinkerGraph.open(conf);
        IoTest.assertModernGraph(reloadedGraph, true, false);
        reloadedGraph.close();
    }

    @Test
    public void shouldPersistToAnyGraphFormat() {
        final String graphLocation = TestHelper.makeTestDataDirectory(TinkerGraphTest.class) + "shouldPersistToAnyGraphFormat.dat";
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) f.delete();

        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, TestIoBuilder.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, graphLocation);
        final TinkerGraph graph = TinkerGraph.open(conf);
        TinkerFactory.generateModern(graph);

        //Test write graph
        graph.close();
        assertEquals(TestIoBuilder.calledRegistry, 0);
        assertEquals(TestIoBuilder.calledOnMapper, 1);
        assertEquals(TestIoBuilder.calledGraph, 1);
        assertEquals(TestIoBuilder.calledCreate, 1);

        try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(f))){
            os.write("dummy string".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Test read graph
        final TinkerGraph readGraph = TinkerGraph.open(conf);
        assertEquals(TestIoBuilder.calledRegistry, 0);
        assertEquals(TestIoBuilder.calledOnMapper, 1);
        assertEquals(TestIoBuilder.calledGraph, 1);
        assertEquals(TestIoBuilder.calledCreate, 1);
    }

    @Test
    public void shouldSerializeWithColorClassResolverToTinkerGraph() throws Exception {
        final Map<String,Color> colors = new HashMap<>();
        colors.put("red", Color.RED);
        colors.put("green", Color.GREEN);

        final ArrayList<Color> colorList = new ArrayList<>(Arrays.asList(Color.RED, Color.GREEN));

        final Supplier<ClassResolver> classResolver = new CustomClassResolverSupplier();
        final GryoMapper mapper = GryoMapper.build().addRegistry(TinkerIoRegistryV1d0.instance()).classResolver(classResolver).create();
        final Kryo kryo = mapper.createMapper();
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);

            kryo.writeObject(out, colorList);
            out.flush();
            final byte[] b = stream.toByteArray();

            try (final InputStream inputStream = new ByteArrayInputStream(b)) {
                final Input input = new Input(inputStream);
                final List m = kryo.readObject(input, ArrayList.class);
                final TinkerGraph readX = (TinkerGraph) m.get(0);
                assertEquals(104, IteratorUtils.count(readX.vertices()));
                assertEquals(102, IteratorUtils.count(readX.edges()));
            }
        }
    }

    @Test
    public void shouldSerializeWithColorClassResolverToTinkerGraphUsingDeprecatedTinkerIoRegistry() throws Exception {
        final Map<String,Color> colors = new HashMap<>();
        colors.put("red", Color.RED);
        colors.put("green", Color.GREEN);

        final ArrayList<Color> colorList = new ArrayList<>(Arrays.asList(Color.RED, Color.GREEN));

        final Supplier<ClassResolver> classResolver = new CustomClassResolverSupplier();
        final GryoMapper mapper = GryoMapper.build().addRegistry(TinkerIoRegistryV1d0.instance()).classResolver(classResolver).create();
        final Kryo kryo = mapper.createMapper();
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);

            kryo.writeObject(out, colorList);
            out.flush();
            final byte[] b = stream.toByteArray();

            try (final InputStream inputStream = new ByteArrayInputStream(b)) {
                final Input input = new Input(inputStream);
                final List m = kryo.readObject(input, ArrayList.class);
                final TinkerGraph readX = (TinkerGraph) m.get(0);
                assertEquals(104, IteratorUtils.count(readX.vertices()));
                assertEquals(102, IteratorUtils.count(readX.edges()));
            }
        }
    }

    /**
     * Coerces a {@code Color} to a {@link TinkerGraph} during serialization.  Demonstrates how custom serializers
     * can be developed that can coerce one value to another during serialization.
     */
    public final static class ColorToTinkerGraphSerializer extends Serializer<Color> {
        public ColorToTinkerGraphSerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final Color color) {
            final TinkerGraph graph = TinkerGraph.open();
            final Vertex v = graph.addVertex(T.id, 1, T.label, "color", "name", color.toString());
            final Vertex vRed = graph.addVertex(T.id, 2, T.label, "primary", "name", "red");
            final Vertex vGreen = graph.addVertex(T.id, 3, T.label, "primary", "name", "green");
            final Vertex vBlue = graph.addVertex(T.id, 4, T.label, "primary", "name", "blue");

            v.addEdge("hasComponent", vRed, "amount", color.getRed());
            v.addEdge("hasComponent", vGreen, "amount", color.getGreen());
            v.addEdge("hasComponent", vBlue, "amount", color.getBlue());

            // make some junk so the graph is kinda big
            generate(graph);

            try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                GryoWriter.build().mapper(() -> kryo).create().writeGraph(stream, graph);
                final byte[] bytes = stream.toByteArray();
                output.writeInt(bytes.length);
                output.write(bytes);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public Color read(final Kryo kryo, final Input input, final Class<Color> colorClass) {
            throw new UnsupportedOperationException("IoX writes to DetachedVertex and can't be read back in as IoX");
        }

        private static void generate(final Graph graph) {
            final int size = 100;
            final List<Object> ids = new ArrayList<>();
            final Vertex v = graph.addVertex("sin", 0.0f, "cos", 1.0f, "ii", 0f);
            ids.add(v.id());

            final GraphTraversalSource g = graph.traversal();

            final Random rand = new Random();
            for (int ii = 1; ii < size; ii++) {
                final Vertex t = graph.addVertex("ii", ii, "sin", Math.sin(ii / 5.0f), "cos", Math.cos(ii / 5.0f));
                final Vertex u = g.V(ids.get(rand.nextInt(ids.size()))).next();
                t.addEdge("linked", u);
                ids.add(u.id());
                ids.add(v.id());
            }
        }
    }

    public static class CustomClassResolverSupplier implements Supplier<ClassResolver> {
        @Override
        public ClassResolver get() {
            return new CustomClassResolver();
        }
    }

    public static class CustomClassResolver extends GryoClassResolver {
        private ColorToTinkerGraphSerializer colorToGraphSerializer = new ColorToTinkerGraphSerializer();

        public Registration getRegistration(final Class clazz) {
            if (Color.class.isAssignableFrom(clazz)) {
                final Registration registration = super.getRegistration(TinkerGraph.class);
                return new Registration(registration.getType(), colorToGraphSerializer, registration.getId());
            } else {
                return super.getRegistration(clazz);
            }
        }
    }

    public static class TestIoBuilder implements Io.Builder {

        static int calledRegistry, calledGraph, calledCreate, calledOnMapper;

        public TestIoBuilder(){
            //Looks awkward to reset static vars inside a constructor, but makes sense from testing perspective
            calledRegistry = 0;
            calledGraph = 0;
            calledCreate = 0;
            calledOnMapper = 0;
        }

        @Override
        public Io.Builder<? extends Io> registry(final IoRegistry registry) {
            calledRegistry++;
            return this;
        }

        @Override
        public Io.Builder<? extends Io> onMapper(final Consumer onMapper) {
            calledOnMapper++;
            return this;
        }

        @Override
        public Io.Builder<? extends Io> graph(final Graph graph) {
            calledGraph++;
            return this;
        }

        @Override
        public Io create() {
            calledCreate++;
            return mock(Io.class);
        }
    }
}
