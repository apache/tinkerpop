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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoClassResolverV1;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
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

        // add the same one twice to check idempotency
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
        final Mapper<ObjectMapper> mapper = graph.io(IoCore.graphson()).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create();
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
        final String graphLocation = TestHelper.makeTestDataFile(TinkerGraphTest.class, "shouldPersistToGraphML.xml");
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
        final String graphLocation = TestHelper.makeTestDataFile(TinkerGraphTest.class, "shouldPersistToGraphSON.json");
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
        final String graphLocation = TestHelper.makeTestDataFile(TinkerGraphTest.class, "shouldPersistToGryo.kryo");
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
        final String graphLocation = TestHelper.makeTestDataFile(TinkerGraphTest.class, "shouldPersistToGryoMulti.kryo");
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
                                                                  TestHelper.makeTestDataPath(TinkerGraphTest.class))
                                     + "shouldPersistToGryoRelative.kryo";
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
        final String graphLocation = TestHelper.makeTestDataFile(TinkerGraphTest.class, "shouldPersistToAnyGraphFormat.dat");
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) f.delete();

        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, TestIoBuilder.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, graphLocation);
        final TinkerGraph graph = TinkerGraph.open(conf);
        TinkerFactory.generateModern(graph);

        //Test write graph
        graph.close();
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
        final GryoMapper mapper = GryoMapper.build().version(GryoVersion.V3_0).addRegistry(TinkerIoRegistryV3.instance()).classResolver(classResolver).create();
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
        final GryoMapper mapper = GryoMapper.build().version(GryoVersion.V3_0).addRegistry(TinkerIoRegistryV3.instance()).classResolver(classResolver).create();
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
    public void shouldCloneTinkergraph() {
        final TinkerGraph original = TinkerGraph.open();
        final TinkerGraph clone = TinkerGraph.open();

        final Vertex marko = original.addVertex("name", "marko", "age", 29);
        final Vertex stephen = original.addVertex("name", "stephen", "age", 35);
        marko.addEdge("knows", stephen);
        GraphHelper.cloneElements(original, clone);

        final Vertex michael = clone.addVertex("name", "michael");
        michael.addEdge("likes", marko);
        michael.addEdge("likes", stephen);
        clone.traversal().V().property("newProperty", "someValue").toList();
        clone.traversal().E().property("newProperty", "someValue").toList();

        assertEquals("original graph should be unchanged", new Long(2), original.traversal().V().count().next());
        assertEquals("original graph should be unchanged", new Long(1), original.traversal().E().count().next());
        assertEquals("original graph should be unchanged", new Long(0), original.traversal().V().has("newProperty").count().next());

        assertEquals("cloned graph should contain new elements", new Long(3), clone.traversal().V().count().next());
        assertEquals("cloned graph should contain new elements", new Long(3), clone.traversal().E().count().next());
        assertEquals("cloned graph should contain new property", new Long(3), clone.traversal().V().has("newProperty").count().next());
        assertEquals("cloned graph should contain new property", new Long(3), clone.traversal().E().has("newProperty").count().next());

        assertNotSame("cloned elements should reference to different objects",
            original.traversal().V().has("name", "stephen").next(),
            clone.traversal().V().has("name", "stephen").next());
    }

    /**
     * This isn't a TinkerGraph specific test, but TinkerGraph is probably best suited for the testing of this
     * particular problem originally noted in TINKERPOP-1992.
     */
    @Test
    public void shouldProperlyTimeReducingBarrierForProfile() {
        final GraphTraversalSource g = TinkerFactory.createModern().traversal();

        TraversalMetrics m = g.V().group().by().by(__.bothE().count()).profile().next();
        for (Metrics i : m.getMetrics(1).getNested()) {
            assertThat(i.getDuration(TimeUnit.NANOSECONDS), greaterThan(0L));
        }

        m = g.withComputer().V().group().by().by(__.bothE().count()).profile().next();
        for (Metrics i : m.getMetrics(1).getNested()) {
            assertThat(i.getDuration(TimeUnit.NANOSECONDS), greaterThan(0L));
        }
    }

    /**
     * Just validating that property folding works nicely given TINKERPOP-2112
     */
    @Test
    public void shouldFoldPropertyStepForTokens() {
        final GraphTraversalSource g = TinkerGraph.open().traversal();

        g.addV("person").property(VertexProperty.Cardinality.single, "k", "v").
                property(T.id , "id").
                property(VertexProperty.Cardinality.list, "l", 1).
                property("x", "y").
                property(VertexProperty.Cardinality.list, "l", 2).
                property("m", "m", "mm", "mm").
                property("y", "z").iterate();

        assertThat(g.V("id").hasNext(), is(true));
    }

    /**
     * Validating that start-step hasId() unwraps ids in lists in addition to ids in arrays as per TINKERPOP-2863
     */
    @Test
    public void shouldCheckWithinListsOfIdsForStartStepHasId() {
        final GraphTraversalSource g = TinkerFactory.createModern().traversal();

        final List<Vertex> expectedStartTraversal = g.V().hasId(1, 2).toList();

        assertEquals(expectedStartTraversal, g.V().hasId(new Integer[]{1, 2}).toList());
        assertEquals(expectedStartTraversal,g.V().hasId(Arrays.asList(1, 2)).toList());
    }

    /**
     * Validating that mid-traversal hasId() also unwraps ids in lists in addition to ids in arrays as per TINKERPOP-2863
     */
    @Test
    public void shouldCheckWithinListsOfIdsForMidTraversalHasId() {
        final GraphTraversalSource g = TinkerFactory.createModern().traversal();

        final List<Vertex> expectedMidTraversal = g.V().has("name", "marko").outE("knows").inV().hasId(2, 4).toList();

        assertEquals(expectedMidTraversal, g.V().has("name", "marko").outE("knows").inV().hasId(new Integer[]{2, 4}).toList());
        assertEquals(expectedMidTraversal, g.V().has("name", "marko").outE("knows").inV().hasId(Arrays.asList(2, 4)).toList());
    }

    @Test
    public void shouldOptionalUsingWithComputer() {
        // not all systems will have 3+ available processors (e.g. travis)
        assumeThat(Runtime.getRuntime().availableProcessors(), greaterThan(2));

        // didn't add this as a general test as it basically was only failing under a specific condition for
        // TinkerGraphComputer - see more here: https://issues.apache.org/jira/browse/TINKERPOP-1619
        final GraphTraversalSource g = TinkerFactory.createModern().traversal();

        final List<Edge> expected = g.E(7, 7, 8, 9).order().by(T.id).toList();
        assertEquals(expected, g.withComputer(Computer.compute().workers(3)).V(1, 2).optional(__.bothE().dedup()).order().by(T.id).toList());
        assertEquals(expected, g.withComputer(Computer.compute().workers(4)).V(1, 2).optional(__.bothE().dedup()).order().by(T.id).toList());
    }

    @Test
    public void shouldReservedKeyVerify() {
        final Set<String> reserved = new HashSet<>(Arrays.asList("something", "id", "label"));
        final GraphTraversalSource g = TinkerGraph.open().traversal().withStrategies(
                ReservedKeysVerificationStrategy.build().reservedKeys(reserved).throwException().create());

        g.addV("person").property(T.id, 123).iterate();

        try {
            g.addV("person").property("id", 123).iterate();
            fail("Verification exception expected");
        } catch (IllegalStateException ve) {
            assertThat(ve.getMessage(), containsString("that is setting a property key to a reserved word"));
        }

        try {
            g.addV("person").property("something", 123).iterate();
            fail("Verification exception expected");
        } catch (IllegalStateException ve) {
            assertThat(ve.getMessage(), containsString("that is setting a property key to a reserved word"));
        }
    }

    @Test
    public void shouldProvideClearErrorWhenTryingToMutateT() {
        final GraphTraversalSource g = TinkerGraph.open().traversal();
        g.addV("person").property(T.id, 100).iterate();

        try {
            g.V(100).property(T.label, "software").iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("T.label is immutable on existing elements", ise.getMessage());
        }

        try {
            g.V(100).property(T.id, 101).iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("T.id is immutable on existing elements", ise.getMessage());
        }

        try {
            g.V(100).property("name", "marko").property(T.label, "software").iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("T.label is immutable on existing elements", ise.getMessage());
        }

        try {
            g.V(100).property(T.id, 101).property("name", "marko").iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("T.id is immutable on existing elements", ise.getMessage());
        }
    }

    @Test
    public void shouldProvideClearErrorWhenTryingToMutateEdgeWithCardinality() {
        final GraphTraversalSource g = TinkerFactory.createModern().traversal();

        try {
            g.E().property(VertexProperty.Cardinality.single, "k", 100).iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("Property cardinality can only be set for a Vertex but the traversal encountered TinkerEdge for key: k", ise.getMessage());
        }

        try {
            g.E().property(VertexProperty.Cardinality.list, "k", 100).iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("Property cardinality can only be set for a Vertex but the traversal encountered TinkerEdge for key: k", ise.getMessage());
        }

        try {
            g.addE("link").to(__.V(1)).from(__.V(1)).
                    property(VertexProperty.Cardinality.list, "k", 100).iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("Multi-property cardinality of [list] can only be set for a Vertex but is being used for addE() with key: k", ise.getMessage());
        }
    }

    @Test
    public void shouldProvideClearErrorWhenPuttingFromToInWrongSpot() {
        final GraphTraversalSource g = TinkerFactory.createModern().traversal();

        try {
            g.addE("link").property(VertexProperty.Cardinality.single, "k", 100).out().
                    to(__.V(1)).from(__.V(1)).iterate();
            fail("Should have thrown an error");
        } catch (IllegalArgumentException ise) {
            assertEquals("The to() step cannot follow VertexStep", ise.getMessage());
        }

        try {
            g.addE("link").property("k", 100).out().
                    from(__.V(1)).to(__.V(1)).iterate();
            fail("Should have thrown an error");
        } catch (IllegalArgumentException ise) {
            assertEquals("The from() step cannot follow VertexStep", ise.getMessage());
        }
    }

    @Test
    public void shouldProvideClearErrorWhenFromOrToDoesNotResolveToVertex() {
        final GraphTraversalSource g = TinkerFactory.createModern().traversal();

        try {
            g.addE("link").property(VertexProperty.Cardinality.single, "k", 100).to(__.V(1)).iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("The value given to addE(link).from() must resolve to a Vertex but null was specified instead", ise.getMessage());
        }

        try {
            g.addE("link").property(VertexProperty.Cardinality.single, "k", 100).from(__.V(1)).iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("The value given to addE(link).to() must resolve to a Vertex but null was specified instead", ise.getMessage());
        }

        try {
            g.addE("link").property("k", 100).from(__.V(1)).iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("The value given to addE(link).to() must resolve to a Vertex but null was specified instead", ise.getMessage());
        }

        try {
            g.V(1).values("name").as("a").addE("link").property(VertexProperty.Cardinality.single, "k", 100).from("a").iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("The value given to addE(link).to() must resolve to a Vertex but String was specified instead", ise.getMessage());
        }

        try {
            g.V(1).values("name").as("a").addE("link").property(VertexProperty.Cardinality.single, "k", 100).to("a").iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("The value given to addE(link).to() must resolve to a Vertex but String was specified instead", ise.getMessage());
        }

        try {
            g.V(1).as("v").values("name").as("a").addE("link").property(VertexProperty.Cardinality.single, "k", 100).to("v").from("a").iterate();
            fail("Should have thrown an error");
        } catch (IllegalStateException ise) {
            assertEquals("The value given to addE(link).to() must resolve to a Vertex but String was specified instead", ise.getMessage());
        }
    }

    @Test
    public void shouldWorkWithoutIdentityStrategy() {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = traversal().withEmbedded(graph).withoutStrategies(IdentityRemovalStrategy.class);
        final List<Map<String,Object>> result = g.V().match(__.as("a").out("knows").values("name").as("b")).identity().toList();
        assertEquals(2, result.size());
        result.stream().forEach(m -> {
            assertEquals(2, m.size());
            assertThat(m.containsKey("a"), is(true));
            assertThat(m.containsKey("b"), is(true));
        });
    }

    @Test
    public void shouldApplyStrategiesRecursivelyWithGraph() {
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph).withStrategies(new TraversalStrategy.ProviderOptimizationStrategy() {
            @Override
            public void apply(final Traversal.Admin<?, ?> traversal) {
                final Graph graph = traversal.getGraph().get();
                graph.addVertex("person");
            }
        });

        // adds one person by way of the strategy
        g.inject(0).iterate();
        assertEquals(1, traversal().withEmbedded(graph).V().hasLabel("person").count().next().intValue());

        // adds two persons by way of the strategy one for the parent and one for the child
        g.inject(0).sideEffect(__.addV()).iterate();
        assertEquals(3, traversal().withEmbedded(graph).V().hasLabel("person").count().next().intValue());
    }

    @Test
    public void shouldAllowHeterogeneousIdsWithAnyManager() {
        final Configuration anyManagerConfig = new BaseConfiguration();
        anyManagerConfig.addProperty(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, TinkerGraph.DefaultIdManager.ANY.name());
        anyManagerConfig.addProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, TinkerGraph.DefaultIdManager.ANY.name());
        anyManagerConfig.addProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, TinkerGraph.DefaultIdManager.ANY.name());
        final Graph graph = TinkerGraph.open(anyManagerConfig);
        final GraphTraversalSource g = traversal().withEmbedded(graph);

        final UUID uuid = UUID.fromString("0E939658-ADD2-4598-A722-2FC178E9B741");
        g.addV("person").property(T.id, 100).
                addV("person").property(T.id, "1000").
                addV("person").property(T.id, "1001").
                addV("person").property(T.id, uuid).iterate();

        assertEquals(3, g.V(100, "1000", uuid).count().next().intValue());
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

    public static class CustomClassResolver extends GryoClassResolverV1 {
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

        static int calledGraph, calledCreate, calledOnMapper;

        public TestIoBuilder(){
            //Looks awkward to reset static vars inside a constructor, but makes sense from testing perspective
            calledGraph = 0;
            calledCreate = 0;
            calledOnMapper = 0;
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

        @Override
        public boolean requiresVersion(final Object version) {
            return false;
        }
    }
}
