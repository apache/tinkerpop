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

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.io.GraphMigrator;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.LegacyGraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.io.gryo.VertexByteArrayInputStream;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.StreamFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_ANY_IDS;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures.FEATURE_VARIABLES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IoTest extends AbstractGremlinTest {

    private static final String GRAPHML_RESOURCE_PATH_PREFIX = "/org/apache/tinkerpop/gremlin/structure/io/graphml/";
    private static final String GRAPHSON_RESOURCE_PATH_PREFIX = "/org/apache/tinkerpop/gremlin/structure/io/graphson/";

    private static String tempPath;

    static {
        tempPath = TestHelper.makeTestDataPath(IoTest.class, "iotest").getPath() + File.separator;
    }

    @BeforeClass
    public static void before() throws IOException {
        final File tempDir = new File(tempPath);
        FileUtils.deleteDirectory(tempDir);
        if (!tempDir.mkdirs()) throw new IOException(String.format("Could not create %s", tempDir));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadGraphML() throws IOException {
        readGraphMLIntoGraph(graph);
        assertClassicGraph(graph, false, true);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_BOOLEAN_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_LONG_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadGraphMLAnAllSupportedDataTypes() throws IOException {
        final GraphReader reader = GraphMLReader.build().create();
        try (final InputStream stream = IoTest.class.getResourceAsStream(GRAPHML_RESOURCE_PATH_PREFIX + "graph-types.xml")) {
            reader.readGraph(stream, graph);
        }

        final Vertex v = graph.vertices().next();
        assertEquals(123.45d, v.value("d"), 0.000001d);
        assertEquals("some-string", v.<String>value("s"));
        assertEquals(29, v.<Integer>value("i").intValue());
        assertEquals(true, v.<Boolean>value("b"));
        assertEquals(123.54f, v.value("f"), 0.000001f);
        assertEquals(10000000l, v.<Long>value("l").longValue());
        assertEquals("junk", v.<String>value("n"));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteClassicToGraphMLToFileWithHelpers() throws Exception {
        final File f = TestHelper.generateTempFile(this.getClass(), name.getMethodName(), ".xml");
        try {
            graph.io().writeGraphML(f.getAbsolutePath());

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.CLASSIC);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            g1.io().readGraphML(f.getAbsolutePath());

            assertClassicGraph(graph, false, true);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        } catch (Exception ex) {
            f.delete();
            throw ex;
        }
    }

    /**
     * Only need to execute this test with TinkerGraph or other graphs that support user supplied identifiers.
     */
    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldWriteNormalizedGraphML() throws Exception {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            final GraphMLWriter w = GraphMLWriter.build().normalize(true).create();
            w.writeGraph(bos, graph);

            final String expected = streamToString(IoTest.class.getResourceAsStream(GRAPHML_RESOURCE_PATH_PREFIX + "tinkerpop-classic-normalized.xml"));
            assertEquals(expected.replace("\n", "").replace("\r", ""), bos.toString().replace("\n", "").replace("\r", ""));
        }
    }

    /**
     * Only need to execute this test with TinkerGraph or other graphs that support user supplied identifiers.
     */
    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = FEATURE_VARIABLES)
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldWriteNormalizedGraphSON() throws Exception {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            final GraphSONMapper mapper = graph.io().graphSONMapper().normalize(true).create();
            final GraphSONWriter w = graph.io().graphSONWriter().mapper(mapper).create();
            w.writeGraph(bos, graph);

            final String expected = streamToString(IoTest.class.getResourceAsStream(GRAPHSON_RESOURCE_PATH_PREFIX + "tinkerpop-classic-normalized.json"));
            assertEquals(expected.replace("\n", "").replace("\r", ""), bos.toString().replace("\n", "").replace("\r", ""));
        }
    }

    /**
     * Note: this is only a very lightweight test of writer/reader encoding. It is known that there are characters
     * which, when written by GraphMLWriter, cause parse errors for GraphMLReader. However, this happens uncommonly
     * enough that is not yet known which characters those are. Only need to execute this test with TinkerGraph
     * or other graphs that support user supplied identifiers.
     */
    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS)
    public void shouldProperlyEncodeWithGraphML() throws Exception {
        final Vertex v = graph.addVertex(T.id, "1");
        v.property(VertexProperty.Cardinality.single, "text", "\u00E9");

        final GraphMLWriter w = GraphMLWriter.build().create();

        final File f = TestHelper.generateTempFile(this.getClass(), "test", ".txt");
        try (final OutputStream out = new FileOutputStream(f)) {
            w.writeGraph(out, graph);
        }

        validateXmlAgainstGraphMLXsd(f);

        // reusing the same config used for creation of "g".
        final Configuration configuration = graphProvider.newGraphConfiguration("g2", this.getClass(), name.getMethodName(), null);
        graphProvider.clear(configuration);
        final Graph g2 = graphProvider.openTestGraph(configuration);
        final GraphMLReader r = GraphMLReader.build().create();

        try (final InputStream in = new FileInputStream(f)) {
            r.readGraph(in, g2);
        }

        final Vertex v2 = g2.vertices("1").next();
        assertEquals("\u00E9", v2.property("text").value());

        // need to manually close the "g2" instance
        graphProvider.clear(g2, configuration);
    }

    /**
     * This is just a serialization check.
     */
    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ANY_IDS)
    public void shouldProperlySerializeCustomIdWithGraphSON() throws Exception {
        final UUID id = UUID.fromString("AF4B5965-B176-4552-B3C1-FBBE2F52C305");
        graph.addVertex(T.id, new CustomId("vertex", id));

        final SimpleModule module = new SimpleModule();
        module.addSerializer(CustomId.class, new CustomId.CustomIdJacksonSerializer());
        final GraphWriter writer = graph.io().graphSONWriter().mapper(
                graph.io().graphSONMapper().addCustomModule(module).embedTypes(true).create()).create();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            writer.writeGraph(baos, graph);

            final JsonNode jsonGraph = new ObjectMapper().readTree(baos.toByteArray());
            final JsonNode onlyVertex = jsonGraph.findValues(GraphSONTokens.VERTICES).get(0).get(0);
            final JsonNode idValue = onlyVertex.get(GraphSONTokens.ID);
            assertTrue(idValue.has("cluster"));
            assertEquals("vertex", idValue.get("cluster").asText());
            assertTrue(idValue.has("elementId"));
            assertEquals("AF4B5965-B176-4552-B3C1-FBBE2F52C305".toLowerCase(), idValue.get("elementId").asText());

            // reusing the same config used for creation of "g".
            final Configuration configuration = graphProvider.newGraphConfiguration("g2", this.getClass(), name.getMethodName(), null);
            graphProvider.clear(configuration);
            final Graph g2 = graphProvider.openTestGraph(configuration);

            try (final InputStream is = new ByteArrayInputStream(baos.toByteArray())) {
                final GraphReader reader = graph.io().graphSONReader()
                        .mapper(graph.io().graphSONMapper().embedTypes(true).addCustomModule(module).create()).create();
                reader.readGraph(is, g2);
            }

            final Vertex v2 = g2.vertices().next();
            final CustomId customId = (CustomId) v2.id();
            assertEquals(id, customId.getElementId());
            assertEquals("vertex", customId.getCluster());

            // need to manually close the "g2" instance
            graphProvider.clear(g2, configuration);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ANY_IDS)
    public void shouldProperlySerializeCustomIdWithGryo() throws Exception {
        graph.addVertex(T.id, new CustomId("vertex", UUID.fromString("AF4B5965-B176-4552-B3C1-FBBE2F52C305")));
        final GryoMapper gryo = GryoMapper.build().addCustom(CustomId.class).create();

        final GryoWriter writer = GryoWriter.build().mapper(gryo).create();
        final GryoReader reader = GryoReader.build().workingDirectory(tempPath).mapper(gryo).create();

        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), null);
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        GraphMigrator.migrateGraph(graph, g1, reader, writer);

        final Vertex onlyVertex = g1.traversal().V().next();
        final CustomId id = (CustomId) onlyVertex.id();
        assertEquals("vertex", id.getCluster());
        assertEquals(UUID.fromString("AF4B5965-B176-4552-B3C1-FBBE2F52C305"), id.getElementId());

        // need to manually close the "g1" instance
        graphProvider.clear(g1, configuration);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldMigrateGraphWithFloat() throws Exception {
        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), null);
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
        final GryoWriter writer = graph.io().gryoWriter().create();

        GraphMigrator.migrateGraph(graph, g1, reader, writer);

        assertClassicGraph(g1, false, false);

        // need to manually close the "g1" instance
        graphProvider.clear(g1, configuration);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldMigrateGraph() throws Exception {
        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.MODERN);
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
        final GryoWriter writer = graph.io().gryoWriter().create();

        GraphMigrator.migrateGraph(graph, g1, reader, writer);

        // by making this lossy for float it will assert floats for doubles
        assertModernGraph(g1, true, false);

        // need to manually close the "g1" instance
        graphProvider.clear(g1, configuration);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteModernToGryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeGraph(os, graph);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.MODERN);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            // by making this lossy for float it will assert floats for doubles
            assertModernGraph(g1, true, false);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteModernToGryoToFileWithHelpers() throws Exception {
        final File f = TestHelper.generateTempFile(this.getClass(), name.getMethodName(), ".kryo");
        try {
            graph.io().writeGryo(f.getAbsolutePath());

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.MODERN);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            g1.io().readGryo(f.getAbsolutePath());

            // by making this lossy for float it will assert floats for doubles
            assertModernGraph(g1, true, false);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        } catch (Exception ex) {
            f.delete();
            throw ex;
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteCrewToGryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {

            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeGraph(os, graph);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.CREW);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GryoReader reader = GryoReader.build()
                    .mapper(graph.io().gryoMapper().create())
                    .workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            // by making this lossy for float it will assert floats for doubles
            assertCrewGraph(g1, false);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }


    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteClassicToGryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeGraph(os, graph);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.CLASSIC);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            assertClassicGraph(g1, false, false);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteClassicToGraphSON() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeGraph(os, graph);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.CLASSIC);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            assertClassicGraph(g1, true, false);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteModernToGraphSON() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeGraph(os, graph);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.MODERN);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            assertModernGraph(g1, true, false);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteModernToGraphSONWithHelpers() throws Exception {
        final File f = TestHelper.generateTempFile(this.getClass(), name.getMethodName(), ".json");
        try {
            graph.io().writeGraphSON(f.getAbsolutePath());

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.MODERN);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            g1.io().readGraphSON(f.getAbsolutePath());

            assertModernGraph(g1, true, false);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        } catch (Exception ex) {
            f.delete();
            throw ex;
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteCrewToGraphSON() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeGraph(os, graph);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.CREW);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            assertCrewGraph(g1, true);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteEdgeToGryoUsingFloatProperty() throws Exception {
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5f, "acl", "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.outVertex().id());
                    assertEquals(v2.id(), detachedEdge.inVertex().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(0.5f, detachedEdge.properties("weight").next().value());
                    assertEquals("rw", detachedEdge.properties("acl").next().value());

                    called.set(true);

                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteEdgeToGryo() throws Exception {
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5d, "acl", "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.outVertex().id());
                    assertEquals(v2.id(), detachedEdge.inVertex().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(0.5d, e.properties("weight").next().value());
                    assertEquals("rw", e.properties("acl").next().value());
                    called.set(true);
                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedEdgeAsReferenceToGryo() throws Exception {
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = DetachedFactory.detach(v1.addEdge("friend", v2, "weight", 0.5d, "acl", "rw"), false);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.outVertex().id());
                    assertEquals(v2.id(), detachedEdge.inVertex().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.keys().size(), IteratorUtils.count(detachedEdge.properties()));
                    called.set(true);

                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedEdgeToGryo() throws Exception {
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = DetachedFactory.detach(v1.addEdge("friend", v2, "weight", 0.5d, "acl", "rw"), true);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.outVertex().id());
                    assertEquals(v2.id(), detachedEdge.inVertex().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(0.5d, detachedEdge.properties("weight").next().value());
                    assertEquals("rw", detachedEdge.properties("acl").next().value());
                    called.set(true);
                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteEdgeToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5f, "acl", "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), graph.edges(detachedEdge.id().toString()).next().id());
                    assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                    assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(0.5d, detachedEdge.properties("weight").next().value());
                    assertEquals("rw", detachedEdge.properties("acl").next().value());
                    called.set(true);
                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedEdgeAsReferenceToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = DetachedFactory.detach(v1.addEdge("friend", v2, "weight", 0.5f, "acl", "rw"), false);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), graph.edges(detachedEdge.id().toString()).next().id());
                    assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                    assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.keys().size(), IteratorUtils.count(detachedEdge.properties()));
                    called.set(true);
                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedEdgeToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = DetachedFactory.detach(v1.addEdge("friend", v2, "weight", 0.5f, "acl", "rw"), true);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), graph.edges(detachedEdge.id().toString()).next().id());
                    assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                    assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(0.5d, detachedEdge.properties("weight").next().value());
                    assertEquals("rw", detachedEdge.properties("acl").next().value());
                    called.set(true);
                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldReadWriteEdgeToGraphSONNonLossy() throws Exception {
        final Vertex v1 = graph.addVertex(T.id, 1l, T.label, "person");
        final Vertex v2 = graph.addVertex(T.id, 2l, T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5f, "acl", "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().mapper(graph.io().graphSONMapper().embedTypes(true).create()).create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().mapper(graph.io().graphSONMapper().embedTypes(true).create()).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.outVertex().id());
                    assertEquals(v2.id(), detachedEdge.inVertex().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(0.5f, detachedEdge.properties("weight").next().value());
                    assertEquals("rw", detachedEdge.properties("acl").next().value());
                    called.set(true);

                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_SERIALIZABLE_VALUES)
    public void shouldSupportUUIDInGraphSON() throws Exception {
        final UUID id = UUID.randomUUID();
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "uuid", id);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().mapper(graph.io().graphSONMapper().embedTypes(true).create()).create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().mapper(graph.io().graphSONMapper().embedTypes(true).create()).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    // a quick reminder here that the purpose of these id assertions is to ensure that those with
                    // complex ids that are not simply toString'd (i.e. are complex objects in JSON as well)
                    // properly respond to filtering in Graph.edges/vertices
                    assertEquals(e.id(), graph.edges(detachedEdge.id()).next().id());
                    assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id()).next().id());
                    assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id()).next().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.keys().size(), IteratorUtils.count(detachedEdge.properties()));
                    assertEquals(id, detachedEdge.value("uuid"));

                    called.set(true);

                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_SERIALIZABLE_VALUES)
    public void shouldSupportUUIDInGryo() throws Exception {
        final UUID id = UUID.randomUUID();
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "uuid", id);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.outVertex().id());
                    assertEquals(v2.id(), detachedEdge.inVertex().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.keys().size(), IteratorUtils.count(detachedEdge.properties()));
                    assertEquals(id, detachedEdge.value("uuid"));

                    called.set(true);

                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexNoEdgesToGryoUsingFloatProperty() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "acl", "rw");

        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(2, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                    assertEquals(v1.value("acl"), detachedVertex.value("acl").toString());
                    called.set(true);
                    return mock(Vertex.class);
                });
            }
            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexNoEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "acl", "rw");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(2, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                    assertEquals(v1.value("acl"), detachedVertex.value("acl").toString());
                    called.set(true);
                    return mock(Vertex.class);
                });
            }
            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedVertexNoEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "acl", "rw");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            final DetachedVertex dv = DetachedFactory.detach(v1, true);
            writer.writeVertex(os, dv);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(2, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals("marko", detachedVertex.properties("name").next().value());
                    assertEquals("rw", detachedVertex.properties("acl").next().value());
                    called.set(true);
                    return mock(Vertex.class);
                });
            }
            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedVertexAsReferenceNoEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "acl", "rw");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            final DetachedVertex dv = DetachedFactory.detach(v1, false);
            writer.writeVertex(os, dv);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(0, IteratorUtils.count(detachedVertex.properties()));
                    called.set(true);
                    return mock(Vertex.class);
                });
            }
            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldReadWriteVertexMultiPropsNoEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "name", "mark", "acl", "rw");
        v1.property(VertexProperty.Cardinality.single, "propsSquared", 123, "x", "a", "y", "b");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(4, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals("a", detachedVertex.property("propsSquared").value("x"));
                    assertEquals("b", detachedVertex.property("propsSquared").value("y"));
                    assertEquals(2, IteratorUtils.count(detachedVertex.properties("name")));
                    assertTrue(StreamFactory.stream(detachedVertex.properties("name")).allMatch(p -> p.key().equals("name") && (p.value().equals("marko") || p.value().equals("mark"))));
                    assertEquals(v1.value("acl"), detachedVertex.value("acl").toString());
                    called.set(true);
                    return mock(Vertex.class);
                });
            }
            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexNoEdgesToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "acl", "rw");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(2, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals("marko", detachedVertex.properties("name").next().value());
                    assertEquals("rw", detachedVertex.properties("acl").next().value());
                    called.set(true);
                    return detachedVertex;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedVertexNoEdgesToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "acl", "rw");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            final DetachedVertex dv = DetachedFactory.detach(v1, true);
            writer.writeVertex(os, dv);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(2, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals("marko", detachedVertex.properties("name").next().value());
                    assertEquals("rw", detachedVertex.properties("acl").next().value());
                    called.set(true);
                    return detachedVertex;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedVertexAsReferenceNoEdgesToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "acl", "rw");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            final DetachedVertex dv = DetachedFactory.detach(v1, false);
            writer.writeVertex(os, dv);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(0, IteratorUtils.count(detachedVertex.properties()));

                    called.set(true);
                    return detachedVertex;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldReadWriteVertexMultiPropsNoEdgesToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "name", "mark", "acl", "rw");
        v1.property(VertexProperty.Cardinality.single, "propsSquared", 123, "x", "a", "y", "b");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(4, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals("a", detachedVertex.property("propsSquared").value("x"));
                    assertEquals("b", detachedVertex.property("propsSquared").value("y"));
                    assertEquals(2, IteratorUtils.count(detachedVertex.properties("name")));
                    assertTrue(StreamFactory.stream(detachedVertex.properties("name")).allMatch(p -> p.key().equals("name") && (p.value().equals("marko") || p.value().equals("mark"))));
                    assertEquals(v1.value("acl"), detachedVertex.value("acl").toString());
                    called.set(true);
                    return mock(Vertex.class);
                });
            }
            assertTrue(called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldReadWriteVerticesNoEdgesToGryoManual() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertices(os, g.V().has("age", Compare.gt, 30));

            final AtomicInteger called = new AtomicInteger(0);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();

            try (final VertexByteArrayInputStream vbais = new VertexByteArrayInputStream(new ByteArrayInputStream(os.toByteArray()))) {
                reader.readVertex(new ByteArrayInputStream(vbais.readVertexBytes().toByteArray()),
                        detachedVertex -> {
                            called.incrementAndGet();
                            return detachedVertex;
                        });

                reader.readVertex(new ByteArrayInputStream(vbais.readVertexBytes().toByteArray()),
                        detachedVertex -> {
                            called.incrementAndGet();
                            return detachedVertex;
                        });
            }

            assertEquals(2, called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldReadWriteVerticesNoEdgesToGryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertices(os, g.V().has("age", Compare.gt, 30));

            final AtomicInteger called = new AtomicInteger(0);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();

            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                final Iterator<Vertex> itty = reader.readVertices(bais, null,
                        detachedVertex -> {
                            called.incrementAndGet();
                            return detachedVertex;
                        }, null);

                assertNotNull(itty.next());
                assertNotNull(itty.next());
                assertFalse(itty.hasNext());
            }

            assertEquals(2, called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldReadWriteVerticesNoEdgesToGraphSONManual() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeVertices(os, g.V().has("age", Compare.gt, 30));

            final AtomicInteger called = new AtomicInteger(0);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            final BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(os.toByteArray())));
            String line = br.readLine();
            reader.readVertex(new ByteArrayInputStream(line.getBytes()),
                    detachedVertex -> {
                        called.incrementAndGet();
                        return mock(Vertex.class);
                    });

            line = br.readLine();
            reader.readVertex(new ByteArrayInputStream(line.getBytes()),
                    detachedVertex -> {
                        called.incrementAndGet();
                        return mock(Vertex.class);
                    });

            assertEquals(2, called.get());
        }
    }


    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithOUTOUTEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1, Direction.OUT);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();

            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.OUT, detachedVertex -> {
                            assertEquals(v1.id(), detachedVertex.id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                            assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                            calledVertex.set(true);
                            return detachedVertex;
                        },
                        detachedEdge -> {
                            assertEquals(e.id(), detachedEdge.id());
                            assertEquals(v1.id(), detachedEdge.outVertex().id());
                            assertEquals(v2.id(), detachedEdge.inVertex().id());
                            assertEquals(v1.label(), detachedEdge.outVertex().label());
                            assertEquals(v2.label(), detachedEdge.inVertex().label());
                            assertEquals(e.label(), detachedEdge.label());
                            assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            assertEquals(0.5d, detachedEdge.value("weight"), 0.00001d);

                            calledEdge.set(true);

                            return detachedEdge;
                        });
            }

            assertTrue(calledVertex.get());
            assertTrue(calledEdge.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithOUTOUTEdgesToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeVertex(os, v1, Direction.OUT);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.OUT, detachedVertex -> {
                            assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                            assertEquals("marko", detachedVertex.value("name"));
                            calledVertex.set(true);
                            return null;
                        },
                        detachedEdge -> {
                            assertEquals(e.id(), graph.edges(detachedEdge.id().toString()).next().id());
                            assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                            assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                            assertEquals(v1.label(), detachedEdge.outVertex().label());
                            assertEquals(v2.label(), detachedEdge.inVertex().label());
                            assertEquals(e.label(), detachedEdge.label());
                            assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);                      // lossy

                            calledEdge.set(true);
                            return null;
                        });
            }

            assertTrue(calledVertex.get());
            assertTrue(calledEdge.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithININEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v2.addEdge("friends", v1, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1, Direction.IN);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);

            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.IN, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                    calledVertex.set(true);

                    return detachedVertex;
                }, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v2.id(), detachedEdge.outVertex().id());
                    assertEquals(v1.id(), detachedEdge.inVertex().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                    assertEquals(0.5d, detachedEdge.value("weight"), 0.00001d);

                    calledEdge.set(true);

                    return detachedEdge;
                });
            }

            assertTrue(calledVertex.get());
            assertTrue(calledEdge.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithININEdgesToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v2.addEdge("friends", v1, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeVertex(os, v1, Direction.IN);
            os.close();

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.IN, detachedVertex -> {
                            assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                            assertEquals("marko", detachedVertex.value("name"));
                            calledVertex.set(true);
                            return null;
                        },
                        detachedEdge -> {
                            assertEquals(e.id(), graph.edges(detachedEdge.id().toString()).next().id());
                            assertEquals(v1.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                            assertEquals(v2.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                            assertEquals(v1.label(), detachedEdge.outVertex().label());
                            assertEquals(v2.label(), detachedEdge.inVertex().label());
                            assertEquals(e.label(), detachedEdge.label());
                            assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);                      // lossy

                            calledEdge.set(true);
                            return null;
                        });
            }

            assertTrue(calledVertex.get());
            assertTrue(calledEdge.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHBOTHEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5d);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge1 = new AtomicBoolean(false);
            final AtomicBoolean calledEdge2 = new AtomicBoolean(false);

            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.BOTH, detachedVertex -> {
                            assertEquals(v1.id(), detachedVertex.id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                            assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                            calledVertex.set(true);

                            return detachedVertex;
                        },
                        detachedEdge -> {
                            if (detachedEdge.id().equals(e1.id())) {
                                assertEquals(v2.id(), detachedEdge.outVertex().id());
                                assertEquals(v1.id(), detachedEdge.inVertex().id());
                                assertEquals(v1.label(), detachedEdge.outVertex().label());
                                assertEquals(v2.label(), detachedEdge.inVertex().label());
                                assertEquals(e1.label(), detachedEdge.label());
                                assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                                assertEquals(0.5d, detachedEdge.value("weight"), 0.00001d);
                                calledEdge1.set(true);
                            } else if (detachedEdge.id().equals(e2.id())) {
                                assertEquals(v1.id(), detachedEdge.outVertex().id());
                                assertEquals(v2.id(), detachedEdge.inVertex().id());
                                assertEquals(v1.label(), detachedEdge.outVertex().label());
                                assertEquals(v2.label(), detachedEdge.inVertex().label());
                                assertEquals(e1.label(), detachedEdge.label());
                                assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                                assertEquals(1.0d, detachedEdge.value("weight"), 0.00001d);
                                calledEdge2.set(true);
                            } else {
                                fail("An edge id generated that does not exist");
                            }

                            return null;
                        });
            }

            assertTrue(calledVertex.get());
            assertTrue(calledEdge1.get());
            assertTrue(calledEdge2.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHBOTHEdgesToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edge1Called = new AtomicBoolean(false);
            final AtomicBoolean edge2Called = new AtomicBoolean(false);

            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.BOTH, detachedVertex -> {
                            assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                            assertEquals("marko", detachedVertex.value("name"));
                            vertexCalled.set(true);
                            return null;
                        },
                        detachedEdge -> {
                            if (graph.edges(detachedEdge.id().toString()).next().id().equals(e1.id())) {
                                assertEquals(e1.id(), graph.edges(detachedEdge.id().toString()).next().id());
                                assertEquals(v1.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                                assertEquals(v2.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                                assertEquals(v1.label(), detachedEdge.outVertex().label());
                                assertEquals(v2.label(), detachedEdge.inVertex().label());
                                assertEquals(e1.label(), detachedEdge.label());
                                assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                                assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);                      // lossy
                                edge1Called.set(true);
                            } else if (graph.edges(detachedEdge.id().toString()).next().id().equals(e2.id())) {
                                assertEquals(e2.id(), graph.edges(detachedEdge.id().toString()).next().id());
                                assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                                assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                                assertEquals(v1.label(), detachedEdge.outVertex().label());
                                assertEquals(v2.label(), detachedEdge.inVertex().label());
                                assertEquals(e2.label(), detachedEdge.label());
                                assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                                assertEquals(1.0d, detachedEdge.value("weight"), 0.000001d);                      // lossy
                                edge2Called.set(true);
                            } else {
                                fail("An edge id generated that does not exist");
                            }

                            return null;
                        });
            }

            assertTrue(vertexCalled.get());
            assertTrue(edge1Called.get());
            assertTrue(edge2Called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHBOTHEdgesToGraphSONWithTypes() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().mapper(graph.io().graphSONMapper().embedTypes(true).create()).create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edge1Called = new AtomicBoolean(false);
            final AtomicBoolean edge2Called = new AtomicBoolean(false);

            final GraphSONReader reader = graph.io().graphSONReader().mapper(graph.io().graphSONMapper().embedTypes(true).create()).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.BOTH, detachedVertex -> {
                    // a quick reminder here that the purpose of these id assertions is to ensure that those with
                    // complex ids that are not simply toString'd (i.e. are complex objects in JSON as well)
                    // properly respond to filtering in Graph.edges/vertices
                    assertEquals(v1.id(), graph.vertices(detachedVertex.id()).next().id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                    vertexCalled.set(true);
                    return null;
                }, detachedEdge -> {
                    // a quick reminder here that the purpose of these id assertions is to ensure that those with
                    // complex ids that are not simply toString'd (i.e. are complex objects in JSON as well)
                    // properly respond to filtering in Graph.edges/vertices
                    if (graph.edges(detachedEdge.id()).next().id().equals(e1.id())) {
                        assertEquals(v2.id(), graph.vertices(detachedEdge.outVertex().id()).next().id());
                        assertEquals(v1.id(), graph.vertices(detachedEdge.inVertex().id()).next().id());
                        assertEquals(v1.label(), detachedEdge.outVertex().label());
                        assertEquals(v2.label(), detachedEdge.inVertex().label());
                        assertEquals(e1.label(), detachedEdge.label());
                        assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                        assertEquals(0.5f, detachedEdge.value("weight"), 0.00001f);
                        edge1Called.set(true);
                    } else if (graph.edges(detachedEdge.id()).next().id().equals(e2.id())) {
                        assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id()).next().id());
                        assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id()).next().id());
                        assertEquals(v1.label(), detachedEdge.outVertex().label());
                        assertEquals(v2.label(), detachedEdge.inVertex().label());
                        assertEquals(e1.label(), detachedEdge.label());
                        assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                        assertEquals(1.0f, detachedEdge.value("weight"), 0.00001f);
                        edge2Called.set(true);
                    } else {
                        fail("An edge id generated that does not exist");
                    }

                    return null;
                });
            }

            assertTrue(vertexCalled.get());
            assertTrue(edge1Called.get());
            assertTrue(edge2Called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHINEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5d);
        v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edge1Called = new AtomicBoolean(false);

            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.IN, detachedVertex -> {
                            assertEquals(v1.id(), detachedVertex.id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                            assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                            vertexCalled.set(true);

                            return detachedVertex;
                        },
                        detachedEdge -> {
                            if (detachedEdge.id().equals(e1.id())) {
                                assertEquals(v2.id(), detachedEdge.outVertex().id());
                                assertEquals(v1.id(), detachedEdge.inVertex().id());
                                assertEquals(v1.label(), detachedEdge.outVertex().label());
                                assertEquals(v2.label(), detachedEdge.inVertex().label());
                                assertEquals(e1.label(), detachedEdge.label());
                                assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                                assertEquals(0.5d, detachedEdge.value("weight"), 0.00001d);
                                edge1Called.set(true);
                            } else {
                                fail("An edge id generated that does not exist");
                            }

                            return null;
                        });
            }

            assertTrue(vertexCalled.get());
            assertTrue(edge1Called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHINEdgesToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.IN, detachedVertex -> {
                    assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals("marko", detachedVertex.value("name"));
                    vertexCalled.set(true);

                    return null;
                }, detachedEdge -> {
                    if (graph.edges(detachedEdge.id().toString()).next().id().equals(e1.id())) {
                        assertEquals(e1.id(), graph.edges(detachedEdge.id().toString()).next().id());
                        assertEquals(v1.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                        assertEquals(v2.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                        assertEquals(v1.label(), detachedEdge.outVertex().label());
                        assertEquals(v2.label(), detachedEdge.inVertex().label());
                        assertEquals(e1.label(), detachedEdge.label());
                        assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                        assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);                      // lossy
                        edgeCalled.set(true);
                    } else {
                        fail("An edge id generated that does not exist");
                    }

                    return null;
                });
            }

            assertTrue(edgeCalled.get());
            assertTrue(vertexCalled.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHOUTEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = graph.addVertex(T.label, "person");
        v2.addEdge("friends", v1, "weight", 0.5d);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.OUT, detachedVertex -> {
                            assertEquals(v1.id(), detachedVertex.id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                            assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                            vertexCalled.set(true);

                            return detachedVertex;
                        },
                        detachedEdge -> {
                            if (detachedEdge.id().equals(e2.id())) {
                                assertEquals(v1.id(), detachedEdge.outVertex().id());
                                assertEquals(v2.id(), detachedEdge.inVertex().id());
                                assertEquals(v1.label(), detachedEdge.outVertex().label());
                                assertEquals(v2.label(), detachedEdge.inVertex().label());
                                assertEquals(e2.label(), detachedEdge.label());
                                assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                                assertEquals(1.0d, detachedEdge.value("weight"), 0.00001d);

                                edgeCalled.set(true);
                            } else {
                                fail("An edge id generated that does not exist");
                            }

                            return null;
                        });
            }

            assertTrue(vertexCalled.get());
            assertTrue(edgeCalled.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHOUTEdgesToGraphSON() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = graph.io().graphSONWriter().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final GraphSONReader reader = graph.io().graphSONReader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.OUT, detachedVertex -> {
                    assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals("marko", detachedVertex.value("name"));
                    vertexCalled.set(true);
                    return null;
                }, detachedEdge -> {
                    if (graph.edges(detachedEdge.id().toString()).next().id().equals(e2.id())) {
                        assertEquals(e2.id(), graph.edges(detachedEdge.id().toString()).next().id());
                        assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                        assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                        assertEquals(v1.label(), detachedEdge.outVertex().label());
                        assertEquals(v2.label(), detachedEdge.inVertex().label());
                        assertEquals(e2.label(), detachedEdge.label());
                        assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                        assertEquals(1.0d, detachedEdge.value("weight"), 0.000001d);                      // lossy
                        edgeCalled.set(true);
                    } else {
                        fail("An edge id generated that does not exist");
                    }

                    return null;
                });
            }

            assertTrue(edgeCalled.get());
            assertTrue(vertexCalled.get());
        }
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithOUTBOTHEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1, Direction.OUT);

            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.BOTH,
                        detachedVertex -> null,
                        detachedEdge -> null);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithINBOTHEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko");
        final Vertex v2 = graph.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1, Direction.IN);

            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.BOTH,
                        detachedVertex -> null,
                        detachedEdge -> null);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithINOUTEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko");
        final Vertex v2 = graph.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1, Direction.IN);

            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        detachedVertex -> null,
                        detachedEdge -> null);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithOUTINEdgesToGryo() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GryoWriter writer = graph.io().gryoWriter().create();
            writer.writeVertex(os, v1, Direction.IN);

            final GryoReader reader = graph.io().gryoReader().workingDirectory(tempPath).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        detachedVertex -> null,
                        detachedEdge -> null);
            }
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadLegacyGraphSON() throws IOException {
        final GraphReader reader = LegacyGraphSONReader.build().create();
        try (final InputStream stream = IoTest.class.getResourceAsStream(GRAPHSON_RESOURCE_PATH_PREFIX + "tinkerpop-classic-legacy.json")) {
            reader.readGraph(stream, graph);
        }

        // the id is lossy in migration because TP2 treated ID as String
        assertClassicGraph(graph, false, true);
    }

    public static void assertCrewGraph(final Graph g1, final boolean lossyForId) {
        assertEquals(new Long(6), g1.traversal().V().count().next());
        assertEquals(new Long(14), g1.traversal().E().count().next());

        assertEquals("marko", g1.variables().get("creator").get().toString());
        assertEquals(2014, g1.variables().get("lastModified").get());
        assertEquals("this graph was created to provide examples and test coverage for tinkerpop3 api advances", g1.variables().get("comment").get().toString());

        final Vertex v1 = (Vertex) g1.traversal().V().has("name", "marko").next();
        assertEquals("person", v1.label());
        assertEquals(2, v1.keys().size());
        assertEquals(4, (int) IteratorUtils.count(v1.properties("location")));
        v1.properties("location").forEachRemaining(vp -> {
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
        assertId(g1, lossyForId, v1, 1);

        final List<Edge> v1Edges = IteratorUtils.list(v1.edges(Direction.BOTH));
        assertEquals(4, v1Edges.size());
        v1Edges.forEach(e -> {
            if (e.inVertex().value("name").equals("gremlin") && e.label().equals("develops")) {
                assertEquals(2009, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 13);
            } else if (e.inVertex().value("name").equals("tinkergraph") && e.label().equals("develops")) {
                assertEquals(2010, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 14);
            } else if (e.inVertex().value("name").equals("gremlin") && e.label().equals("uses")) {
                assertEquals(4, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 15);
            } else if (e.inVertex().value("name").equals("tinkergraph") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 16);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v7 = (Vertex) g1.traversal().V().has("name", "stephen").next();
        assertEquals("person", v7.label());
        assertEquals(2, v7.keys().size());
        assertEquals(3, (int) IteratorUtils.count(v7.properties("location")));
        v7.properties("location").forEachRemaining(vp -> {
            if (vp.value().equals("centreville")) {
                assertEquals(1990, (int) vp.value("startTime"));
                assertEquals(2000, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(vp.properties()));
            } else if (vp.value().equals("dulles")) {
                assertEquals(2000, (int) vp.value("startTime"));
                assertEquals(2006, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(vp.properties()));
            } else if (vp.value().equals("purcellville")) {
                assertEquals(2006, (int) vp.value("startTime"));
                assertEquals(1, (int) IteratorUtils.count(vp.properties()));
            } else {
                fail("Found a value that should be there");
            }
        });
        assertId(g1, lossyForId, v7, 7);

        final List<Edge> v7Edges = IteratorUtils.list(v7.edges(Direction.BOTH));
        assertEquals(4, v7Edges.size());
        v7Edges.forEach(e -> {
            if (e.inVertex().value("name").equals("gremlin") && e.label().equals("develops")) {
                assertEquals(2010, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 17);
            } else if (e.inVertex().value("name").equals("tinkergraph") && e.label().equals("develops")) {
                assertEquals(2011, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 18);
            } else if (e.inVertex().value("name").equals("gremlin") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 19);
            } else if (e.inVertex().value("name").equals("tinkergraph") && e.label().equals("uses")) {
                assertEquals(4, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 20);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v8 = (Vertex) g1.traversal().V().has("name", "matthias").next();
        assertEquals("person", v8.label());
        assertEquals(2, v8.keys().size());
        assertEquals(4, (int) IteratorUtils.count(v8.properties("location")));
        v8.properties("location").forEachRemaining(vp -> {
            if (vp.value().equals("bremen")) {
                assertEquals(2004, (int) vp.value("startTime"));
                assertEquals(2007, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(vp.properties()));
            } else if (vp.value().equals("baltimore")) {
                assertEquals(2007, (int) vp.value("startTime"));
                assertEquals(2011, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(vp.properties()));
            } else if (vp.value().equals("oakland")) {
                assertEquals(2011, (int) vp.value("startTime"));
                assertEquals(2014, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(vp.properties()));
            } else if (vp.value().equals("seattle")) {
                assertEquals(2014, (int) vp.value("startTime"));
                assertEquals(1, (int) IteratorUtils.count(vp.properties()));
            } else {
                fail("Found a value that should be there");
            }
        });
        assertId(g1, lossyForId, v8, 8);

        final List<Edge> v8Edges = IteratorUtils.list(v8.edges(Direction.BOTH));
        assertEquals(3, v8Edges.size());
        v8Edges.forEach(e -> {
            if (e.inVertex().value("name").equals("gremlin") && e.label().equals("develops")) {
                assertEquals(2012, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 21);
            } else if (e.inVertex().value("name").equals("gremlin") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 22);
            } else if (e.inVertex().value("name").equals("tinkergraph") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 23);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v9 = (Vertex) g1.traversal().V().has("name", "daniel").next();
        assertEquals("person", v9.label());
        assertEquals(2, v9.keys().size());
        assertEquals(3, (int) IteratorUtils.count(v9.properties("location")));
        v9.properties("location").forEachRemaining(vp -> {
            if (vp.value().equals("spremberg")) {
                assertEquals(1982, (int) vp.value("startTime"));
                assertEquals(2005, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(vp.properties()));
            } else if (vp.value().equals("kaiserslautern")) {
                assertEquals(2005, (int) vp.value("startTime"));
                assertEquals(2009, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(vp.properties()));
            } else if (vp.value().equals("aachen")) {
                assertEquals(2009, (int) vp.value("startTime"));
                assertEquals(1, (int) IteratorUtils.count(vp.properties()));
            } else {
                fail("Found a value that should be there");
            }
        });
        assertId(g1, lossyForId, v9, 9);

        final List<Edge> v9Edges = IteratorUtils.list(v9.edges(Direction.BOTH));
        assertEquals(2, v9Edges.size());
        v9Edges.forEach(e -> {
            if (e.inVertex().value("name").equals("gremlin") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 24);
            } else if (e.inVertex().value("name").equals("tinkergraph") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 25);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v10 = (Vertex) g1.traversal().V().has("name", "gremlin").next();
        assertEquals("software", v10.label());
        assertEquals(1, v10.keys().size());
        assertId(g1, lossyForId, v10, 10);

        final List<Edge> v10Edges = IteratorUtils.list(v10.edges(Direction.BOTH));
        assertEquals(8, v10Edges.size());
        v10Edges.forEach(e -> {
            if (e.outVertex().value("name").equals("marko") && e.label().equals("develops")) {
                assertEquals(2009, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 13);
            } else if (e.outVertex().value("name").equals("marko") && e.label().equals("uses")) {
                assertEquals(4, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 15);
            } else if (e.outVertex().value("name").equals("stephen") && e.label().equals("develops")) {
                assertEquals(2010, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 17);
            } else if (e.outVertex().value("name").equals("stephen") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 19);
            } else if (e.outVertex().value("name").equals("matthias") && e.label().equals("develops")) {
                assertEquals(2012, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 21);
            } else if (e.outVertex().value("name").equals("matthias") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 22);
            } else if (e.outVertex().value("name").equals("daniel") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 24);
            } else if (e.inVertex().value("name").equals("tinkergraph") && e.label().equals("traverses")) {
                assertEquals(0, e.keys().size());
                assertId(g1, lossyForId, e, 26);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v11 = (Vertex) g1.traversal().V().has("name", "tinkergraph").next();
        assertEquals("software", v11.label());
        assertEquals(1, v11.keys().size());
        assertId(g1, lossyForId, v11, 11);

        final List<Edge> v11Edges = IteratorUtils.list(v11.edges(Direction.BOTH));
        assertEquals(7, v11Edges.size());
        v11Edges.forEach(e -> {
            if (e.outVertex().value("name").equals("marko") && e.label().equals("develops")) {
                assertEquals(2010, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 14);
            } else if (e.outVertex().value("name").equals("marko") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 16);
            } else if (e.outVertex().value("name").equals("stephen") && e.label().equals("develops")) {
                assertEquals(2011, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 18);
            } else if (e.outVertex().value("name").equals("stephen") && e.label().equals("uses")) {
                assertEquals(4, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 20);
            } else if (e.outVertex().value("name").equals("matthias") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 23);
            } else if (e.outVertex().value("name").equals("daniel") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 25);
            } else if (e.outVertex().value("name").equals("gremlin") && e.label().equals("traverses")) {
                assertEquals(0, e.keys().size());
                assertId(g1, lossyForId, e, 26);
            } else {
                fail("Edge not expected");
            }
        });
    }

    public static void assertClassicGraph(final Graph g1, final boolean assertDouble, final boolean lossyForId) {
        assertToyGraph(g1, assertDouble, lossyForId, false);
    }

    public static void assertModernGraph(final Graph g1, final boolean assertDouble, final boolean lossyForId) {
        assertToyGraph(g1, assertDouble, lossyForId, true);
    }

    private static void assertToyGraph(final Graph g1, final boolean assertDouble, final boolean lossyForId, final boolean assertSpecificLabel) {
        assertEquals(6, IteratorUtils.count(g1.vertices()));
        assertEquals(6, IteratorUtils.count(g1.edges()));

        final Vertex v1 = (Vertex) g1.traversal().V().has("name", "marko").next();
        assertEquals(29, v1.<Integer>value("age").intValue());
        assertEquals(2, v1.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v1.label());
        assertId(g1, lossyForId, v1, 1);

        final List<Edge> v1Edges = IteratorUtils.list(v1.edges(Direction.BOTH));
        assertEquals(3, v1Edges.size());
        v1Edges.forEach(e -> {
            if (e.inVertex().value("name").equals("vadas")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(0.5d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.5f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 7);
            } else if (e.inVertex().value("name").equals("josh")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(1.0, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 8);
            } else if (e.inVertex().value("name").equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 9);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v2 = (Vertex) g1.traversal().V().has("name", "vadas").next();
        assertEquals(27, v2.<Integer>value("age").intValue());
        assertEquals(2, v2.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v2.label());
        assertId(g1, lossyForId, v2, 2);

        final List<Edge> v2Edges = IteratorUtils.list(v2.edges(Direction.BOTH));
        assertEquals(1, v2Edges.size());
        v2Edges.forEach(e -> {
            if (e.outVertex().value("name").equals("marko")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(0.5d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.5f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 7);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v3 = (Vertex) g1.traversal().V().has("name", "lop").next();
        assertEquals("java", v3.<String>value("lang"));
        assertEquals(2, v2.keys().size());
        assertEquals(assertSpecificLabel ? "software" : Vertex.DEFAULT_LABEL, v3.label());
        assertId(g1, lossyForId, v3, 3);

        final List<Edge> v3Edges = IteratorUtils.list(v3.edges(Direction.BOTH));
        assertEquals(3, v3Edges.size());
        v3Edges.forEach(e -> {
            if (e.outVertex().value("name").equals("peter")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.2d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.2f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 12);
            } else if (e.outVertex().value("name").equals("josh")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (e.outVertex().value("name").equals("marko")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 9);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v4 = (Vertex) g1.traversal().V().has("name", "josh").next();
        assertEquals(32, v4.<Integer>value("age").intValue());
        assertEquals(2, v4.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v4.label());
        assertId(g1, lossyForId, v4, 4);

        final List<Edge> v4Edges = IteratorUtils.list(v4.edges(Direction.BOTH));
        assertEquals(3, v4Edges.size());
        v4Edges.forEach(e -> {
            if (e.inVertex().value("name").equals("ripple")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 10);
            } else if (e.inVertex().value("name").equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (e.outVertex().value("name").equals("marko")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 8);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v5 = (Vertex) g1.traversal().V().has("name", "ripple").next();
        assertEquals("java", v5.<String>value("lang"));
        assertEquals(2, v5.keys().size());
        assertEquals(assertSpecificLabel ? "software" : Vertex.DEFAULT_LABEL, v5.label());
        assertId(g1, lossyForId, v5, 5);

        final List<Edge> v5Edges = IteratorUtils.list(v5.edges(Direction.BOTH));
        assertEquals(1, v5Edges.size());
        v5Edges.forEach(e -> {
            if (e.outVertex().value("name").equals("josh")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 10);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v6 = (Vertex) g1.traversal().V().has("name", "peter").next();
        assertEquals(35, v6.<Integer>value("age").intValue());
        assertEquals(2, v6.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v6.label());
        assertId(g1, lossyForId, v6, 6);

        final List<Edge> v6Edges = IteratorUtils.list(v6.edges(Direction.BOTH));
        assertEquals(1, v6Edges.size());
        v6Edges.forEach(e -> {
            if (e.inVertex().value("name").equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.2d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.2f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 12);
            } else {
                fail("Edge not expected");
            }
        });
    }

    private static void assertId(final Graph g, final boolean lossyForId, final Element e, final Object expected) {
        if (g.features().edge().supportsUserSuppliedIds()) {
            if (lossyForId)
                assertEquals(expected.toString(), e.id().toString());
            else
                assertEquals(expected, e.id());
        }
    }

    private void validateXmlAgainstGraphMLXsd(final File file) throws Exception {
        final Source xmlFile = new StreamSource(file);
        final SchemaFactory schemaFactory = SchemaFactory
                .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema = schemaFactory.newSchema(IoTest.class.getResource(GRAPHML_RESOURCE_PATH_PREFIX + "graphml-1.1.xsd"));
        final Validator validator = schema.newValidator();
        validator.validate(xmlFile);
    }

    private static void readGraphMLIntoGraph(final Graph g) throws IOException {
        final GraphReader reader = GraphMLReader.build().create();
        try (final InputStream stream = IoTest.class.getResourceAsStream(GRAPHML_RESOURCE_PATH_PREFIX + "tinkerpop-classic.xml")) {
            reader.readGraph(stream, g);
        }
    }

    private String streamToString(final InputStream in) throws IOException {
        final Writer writer = new StringWriter();
        final char[] buffer = new char[1024];
        try (final Reader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"))) {
            int n;
            while ((n = reader.read(buffer)) != -1) {
                writer.write(buffer, 0, n);
            }
        }

        return writer.toString();
    }

    public static class CustomId {
        private String cluster;
        private UUID elementId;

        private CustomId() {
            // required no-arg for gryo serialization
        }

        public CustomId(final String cluster, final UUID elementId) {
            this.cluster = cluster;
            this.elementId = elementId;
        }

        public String getCluster() {
            return cluster;
        }

        public UUID getElementId() {
            return elementId;
        }

        @Override
        public String toString() {
            return cluster + ":" + elementId;
        }

        static class CustomIdJacksonSerializer extends StdSerializer<CustomId> {
            public CustomIdJacksonSerializer() {
                super(CustomId.class);
            }

            @Override
            public void serialize(final CustomId customId, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                    throws IOException, JsonGenerationException {
                // when types are not embedded, stringify or resort to JSON primitive representations of the
                // type so that non-jvm languages can better interoperate with the TinkerPop stack.
                jsonGenerator.writeString(customId.toString());
            }

            @Override
            public void serializeWithType(final CustomId customId, final JsonGenerator jsonGenerator,
                                          final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
                // when the type is included add "class" as a key and then try to utilize as much of the
                // default serialization provided by jackson data-bind as possible.  for example, write
                // the uuid as an object so that when jackson serializes it, it uses the uuid serializer
                // to write it out with the type.  in this way, data-bind should be able to deserialize
                // it back when types are embedded.
                jsonGenerator.writeStartObject();
                jsonGenerator.writeStringField(GraphSONTokens.CLASS, CustomId.class.getName());
                jsonGenerator.writeStringField("cluster", customId.getCluster());
                jsonGenerator.writeObjectField("elementId", customId.getElementId());
                jsonGenerator.writeEndObject();
            }
        }
    }
}
