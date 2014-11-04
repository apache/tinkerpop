package com.tinkerpop.gremlin.structure;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import com.tinkerpop.gremlin.structure.io.GraphMigrator;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.GraphWriter;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import com.tinkerpop.gremlin.structure.io.graphson.LegacyGraphSONReader;
import com.tinkerpop.gremlin.structure.io.kryo.GremlinKryo;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.gremlin.structure.io.kryo.VertexByteArrayInputStream;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.Configuration;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_ANY_IDS;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoTest extends AbstractGremlinTest {

    private static final String GRAPHML_RESOURCE_PATH_PREFIX = "/com/tinkerpop/gremlin/structure/io/graphml/";
    private static final String GRAPHSON_RESOURCE_PATH_PREFIX = "/com/tinkerpop/gremlin/structure/io/graphson/";

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadGraphML() throws IOException {
        readGraphMLIntoGraph(g);
        assertClassicGraph(g, false, true);
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
            reader.readGraph(stream, g);
        }

        final Vertex v = g.V().next();
        assertEquals(123.45d, v.value("d"), 0.000001d);
        assertEquals("some-string", v.<String>value("s"));
        assertEquals(29, v.<Integer>value("i").intValue());
        assertEquals(true, v.<Boolean>value("b"));
        assertEquals(123.54f, v.value("f"), 0.000001f);
        assertEquals(10000000l, v.<Long>value("l").longValue());
        assertEquals("junk", v.<String>value("n"));
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
            w.writeGraph(bos, g);

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
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldWriteNormalizedGraphSON() throws Exception {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            final GraphSONWriter w = GraphSONWriter.build().normalize(true).create();
            w.writeGraph(bos, g);

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
        final Vertex v = g.addVertex(T.id, "1");
        v.property("text", "\u00E9");

        final GraphMLWriter w = GraphMLWriter.build().create();

        final File f = File.createTempFile("test", "txt");
        try (final OutputStream out = new FileOutputStream(f)) {
            w.writeGraph(out, g);
        }

        validateXmlAgainstGraphMLXsd(f);

        // reusing the same config used for creation of "g".
        final Configuration configuration = graphProvider.newGraphConfiguration(
                "g2", this.getClass(), name.getMethodName());
        graphProvider.clear(configuration);
        final Graph g2 = graphProvider.openTestGraph(configuration);
        final GraphMLReader r = GraphMLReader.build().create();

        try (final InputStream in = new FileInputStream(f)) {
            r.readGraph(in, g2);
        }

        final Vertex v2 = g2.v("1");
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
    public void shouldProperlySerializeDeserializeCustomIdWithGraphSON() throws Exception {
        final UUID id = UUID.fromString("AF4B5965-B176-4552-B3C1-FBBE2F52C305");
        g.addVertex(T.id, new CustomId("vertex", id));
        final SimpleModule module = new SimpleModule();
        module.addSerializer(CustomId.class, new CustomId.CustomIdJacksonSerializer());
        module.addDeserializer(CustomId.class, new CustomId.CustomIdJacksonDeserializer());
        final GraphWriter writer = GraphSONWriter.build()
                .embedTypes(true)
                .customModule(module).create();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            writer.writeGraph(baos, g);

            final JsonNode jsonGraph = new ObjectMapper().readTree(baos.toByteArray());
            final JsonNode onlyVertex = jsonGraph.findValues(GraphSONTokens.VERTICES).get(0).get(0);
            final JsonNode idValue = onlyVertex.get(GraphSONTokens.ID);
            assertTrue(idValue.has("cluster"));
            assertEquals("vertex", idValue.get("cluster").asText());
            assertTrue(idValue.has("elementId"));
            assertEquals("AF4B5965-B176-4552-B3C1-FBBE2F52C305".toLowerCase(), idValue.get("elementId").asText());

            // reusing the same config used for creation of "g".
            final Configuration configuration = graphProvider.newGraphConfiguration(
                    "g2", this.getClass(), name.getMethodName());
            graphProvider.clear(configuration);
            final Graph g2 = graphProvider.openTestGraph(configuration);

            try (final InputStream is = new ByteArrayInputStream(baos.toByteArray())) {
                final GraphReader reader = GraphSONReader.build()
                        .embedTypes(true)
                        .customModule(module).create();
                reader.readGraph(is, g2);
            }

            final Vertex v2 = g2.V().next();
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
    public void shouldProperlySerializeCustomIdWithKryo() throws Exception {
        g.addVertex(T.id, new CustomId("vertex", UUID.fromString("AF4B5965-B176-4552-B3C1-FBBE2F52C305")));
        final GremlinKryo kryo = GremlinKryo.build().addCustom(CustomId.class).create();

        final KryoWriter writer = KryoWriter.build().custom(kryo).create();
        final KryoReader reader = KryoReader.build().custom(kryo).create();

        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        GraphMigrator.migrateGraph(g, g1, reader, writer);

        final Vertex onlyVertex = g1.V().next();
        final CustomId id = (CustomId) onlyVertex.id();
        assertEquals("vertex", id.getCluster());
        assertEquals(UUID.fromString("AF4B5965-B176-4552-B3C1-FBBE2F52C305"), id.getElementId());

        // need to manually close the "g1" instance
        graphProvider.clear(g1, configuration);
    }

    @org.junit.Ignore
    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldMigrateGraphWithFloat() throws Exception {
        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        GraphMigrator.migrateGraph(g, g1);

        assertClassicGraph(g1, false, false);

        // need to manually close the "g1" instance
        graphProvider.clear(g1, configuration);
    }

    @org.junit.Ignore
    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldMigrateGraph() throws Exception {
        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        GraphMigrator.migrateGraph(g, g1);

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
    public void shouldReadWriteModernToKryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
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
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteCrewToKryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
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
    public void shouldReadWriteClassicToKryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
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
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GraphSONReader reader = GraphSONReader.build().create();
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
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            assertModernGraph(g1, true, false);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
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
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GraphSONReader reader = GraphSONReader.build().create();
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
    public void shouldReadWriteEdgeToKryoUsingFloatProperty() throws Exception {
        final Vertex v1 = g.addVertex(T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5f, Graph.Key.hide("acl"), "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                    assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.hiddenKeys().size(), StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                    assertEquals(e.keys().size(), StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
                    assertEquals(0.5f, detachedEdge.iterators().propertyIterator("weight").next().value());
                    assertEquals("rw", detachedEdge.iterators().hiddenPropertyIterator("acl").next().value());

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
    public void shouldReadWriteEdgeToKryo() throws Exception {
        final Vertex v1 = g.addVertex(T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5d, Graph.Key.hide("acl"), "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                    assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.hiddenKeys().size(), StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                    assertEquals(e.keys().size(), StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
                    assertEquals(0.5d, e.iterators().propertyIterator("weight").next().value());
                    assertEquals("rw", e.iterators().hiddenPropertyIterator("acl").next().value());

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
    public void shouldReadWriteDetachedEdgeAsReferenceToKryo() throws Exception {
        final Vertex v1 = g.addVertex(T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = DetachedEdge.detach(v1.addEdge("friend", v2, "weight", 0.5d, Graph.Key.hide("acl"), "rw"), true);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                    assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.hiddenKeys().size(), StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                    assertEquals(e.keys().size(), StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());

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
    public void shouldReadWriteDetachedEdgeToKryo() throws Exception {
        final Vertex v1 = g.addVertex(T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = DetachedEdge.detach(v1.addEdge("friend", v2, "weight", 0.5d, Graph.Key.hide("acl"), "rw"));

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                    assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.hiddenKeys().size(), StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                    assertEquals(e.keys().size(), StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
                    assertEquals(0.5d, detachedEdge.iterators().propertyIterator("weight").next().value());
                    assertEquals("rw", detachedEdge.iterators().hiddenPropertyIterator("acl").next().value());

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
        final Vertex v1 = g.addVertex(T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5f, Graph.Key.hide("acl"), "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id().toString(), detachedEdge.id().toString()); // lossy
                    assertEquals(v1.id().toString(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id().toString()); // lossy
                    assertEquals(v2.id().toString(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id().toString());  // lossy
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.hiddenKeys().size(), StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                    assertEquals(e.keys().size(), StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
                    assertEquals(0.5d, detachedEdge.iterators().propertyIterator("weight").next().value());
                    assertEquals("rw", detachedEdge.iterators().hiddenPropertyIterator("acl").next().value());

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
        final Vertex v1 = g.addVertex(T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = DetachedEdge.detach(v1.addEdge("friend", v2, "weight", 0.5f, Graph.Key.hide("acl"), "rw"), true);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id().toString(), detachedEdge.id().toString()); // lossy
                    assertEquals(v1.id().toString(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id().toString()); // lossy
                    assertEquals(v2.id().toString(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id().toString());  // lossy
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.hiddenKeys().size(), StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                    assertEquals(e.keys().size(), StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());

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
        final Vertex v1 = g.addVertex(T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = DetachedEdge.detach(v1.addEdge("friend", v2, "weight", 0.5f, Graph.Key.hide("acl"), "rw"));

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id().toString(), detachedEdge.id().toString()); // lossy
                    assertEquals(v1.id().toString(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id().toString()); // lossy
                    assertEquals(v2.id().toString(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id().toString());  // lossy
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.hiddenKeys().size(), StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                    assertEquals(e.keys().size(), StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
                    assertEquals(0.5d, detachedEdge.iterators().propertyIterator("weight").next().value());
                    assertEquals("rw", detachedEdge.iterators().hiddenPropertyIterator("acl").next().value());

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
        final Vertex v1 = g.addVertex(T.id, 1l, T.label, "person");
        final Vertex v2 = g.addVertex(T.id, 2l, T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5f, Graph.Key.hide("acl"), "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build()
                    .embedTypes(true)
                    .create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build()
                    .embedTypes(true)
                    .create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                    assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.hiddenKeys().size(), StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                    assertEquals(e.keys().size(), StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
                    assertEquals(0.5f, detachedEdge.iterators().propertyIterator("weight").next().value());
                    assertEquals("rw", detachedEdge.iterators().hiddenPropertyIterator("acl").next().value());

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
        final Vertex v1 = g.addVertex(T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "uuid", id);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build()
                    .embedTypes(true)
                    .create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build()
                    .embedTypes(true)
                    .create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                    assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.keys().size(), StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
    public void shouldSupportUUIDInKryo() throws Exception {
        final UUID id = UUID.randomUUID();
        final Vertex v1 = g.addVertex(T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "uuid", id);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                    assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(e.hiddenKeys().size(), StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                    assertEquals(e.keys().size(), StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
    public void shouldReadWriteVertexNoEdgesToKryoUsingFloatProperty() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", Graph.Key.hide("acl"), "rw");

        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                    assertEquals(v1.hiddens("acl").value().next().toString(), detachedVertex.value(Graph.Key.hide("acl")).toString());

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
    public void shouldReadWriteVertexNoEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", Graph.Key.hide("acl"), "rw");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                    assertEquals(v1.hiddens("acl").value().next().toString(), detachedVertex.value(Graph.Key.hide("acl")).toString());
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
    public void shouldReadWriteDetachedVertexNoEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", Graph.Key.hide("acl"), "rw");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            final DetachedVertex dv = DetachedVertex.detach(v1);
            writer.writeVertex(os, dv);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals("marko", detachedVertex.iterators().propertyIterator("name").next().value());
                    assertEquals("rw", detachedVertex.iterators().hiddenPropertyIterator("acl").next().value());
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
    public void shouldReadWriteDetachedVertexAsReferenceNoEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", Graph.Key.hide("acl"), "rw");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            final DetachedVertex dv = DetachedVertex.detach(v1, true);
            writer.writeVertex(os, dv);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(0, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
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
    public void shouldReadWriteVertexMultiPropsNoEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", "name", "mark", Graph.Key.hide("acl"), "rw");
        v1.property("propsSquared", 123, "x", "a", "y", "b");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(3, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals("a", detachedVertex.property("propsSquared").value("x"));
                    assertEquals("b", detachedVertex.property("propsSquared").value("y"));
                    assertEquals(2, StreamFactory.stream(detachedVertex.iterators().propertyIterator("name")).count());
                    assertTrue(StreamFactory.stream(detachedVertex.iterators().propertyIterator("name")).allMatch(p -> p.key().equals("name") && (p.value().equals("marko") || p.value().equals("mark"))));
                    assertEquals(v1.hiddens("acl").value().next().toString(), detachedVertex.value(Graph.Key.hide("acl")).toString());
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
        final Vertex v1 = g.addVertex("name", "marko", Graph.Key.hide("acl"), "rw");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id().toString(), detachedVertex.id().toString()); // lossy
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals("marko", detachedVertex.iterators().propertyIterator("name").next().value());
                    assertEquals("rw", detachedVertex.iterators().hiddenPropertyIterator("acl").next().value());

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
        final Vertex v1 = g.addVertex("name", "marko", Graph.Key.hide("acl"), "rw");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            final DetachedVertex dv = DetachedVertex.detach(v1);
            writer.writeVertex(os, dv);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id().toString(), detachedVertex.id().toString()); // lossy
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals("marko", detachedVertex.iterators().propertyIterator("name").next().value());
                    assertEquals("rw", detachedVertex.iterators().hiddenPropertyIterator("acl").next().value());

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
        final Vertex v1 = g.addVertex("name", "marko", Graph.Key.hide("acl"), "rw");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            final DetachedVertex dv = DetachedVertex.detach(v1, true);
            writer.writeVertex(os, dv);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id().toString(), detachedVertex.id().toString()); // lossy
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(0, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());

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
        final Vertex v1 = g.addVertex("name", "marko", "name", "mark", Graph.Key.hide("acl"), "rw");
        v1.property("propsSquared", 123, "x", "a", "y", "b");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, detachedVertex -> {
                    assertEquals(v1.id().toString(), detachedVertex.id().toString()); // lossy
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(3, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals("a", detachedVertex.property("propsSquared").value("x"));
                    assertEquals("b", detachedVertex.property("propsSquared").value("y"));
                    assertEquals(2, StreamFactory.stream(detachedVertex.iterators().propertyIterator("name")).count());
                    assertTrue(StreamFactory.stream(detachedVertex.iterators().propertyIterator("name")).allMatch(p -> p.key().equals("name") && (p.value().equals("marko") || p.value().equals("mark"))));
                    assertEquals(v1.hiddens("acl").value().next().toString(), detachedVertex.value(Graph.Key.hide("acl")).toString());
                    called.set(true);
                    return mock(Vertex.class);
                });
            }
            assertTrue(called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldReadWriteVerticesNoEdgesToKryoManual() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertices(os, g.V().has("age", Compare.gt, 30));

            final AtomicInteger called = new AtomicInteger(0);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();

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
    public void shouldReadWriteVerticesNoEdgesToKryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertices(os, g.V().has("age", Compare.gt, 30));

            final AtomicInteger called = new AtomicInteger(0);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();

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
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertices(os, g.V().has("age", Compare.gt, 30));

            final AtomicInteger called = new AtomicInteger(0);
            final GraphSONReader reader = GraphSONReader.build().create();
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
    public void shouldReadWriteVertexWithOUTOUTEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1, Direction.OUT);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();

            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.OUT, detachedVertex -> {
                            assertEquals(v1.id(), detachedVertex.id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                            assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                            assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                            calledVertex.set(true);
                            return detachedVertex;
                        },
                        detachedEdge -> {
                            assertEquals(e.id(), detachedEdge.id());
                            assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                            assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                            assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                            assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                            assertEquals(e.label(), detachedEdge.label());
                            assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                            assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1, Direction.OUT);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.OUT, detachedVertex -> {
                            assertEquals(v1.id().toString(), detachedVertex.id().toString());  // lossy
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                            assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                            assertEquals("marko", detachedVertex.value("name"));
                            calledVertex.set(true);
                            return null;
                        },
                        detachedEdge -> {
                            assertEquals(e.id().toString(), detachedEdge.id().toString());  // lossy
                            assertEquals(v1.id().toString(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id().toString());  // lossy
                            assertEquals(v2.id().toString(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id().toString());   // lossy
                            assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                            assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                            assertEquals(e.label(), detachedEdge.label());
                            assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                            assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
    public void shouldReadWriteVertexWithININEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = v2.addEdge("friends", v1, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1, Direction.IN);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);

            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.IN, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                    calledVertex.set(true);

                    return detachedVertex;
                }, detachedEdge -> {
                    assertEquals(e.id(), detachedEdge.id());
                    assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                    assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                    assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                    assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                    assertEquals(e.label(), detachedEdge.label());
                    assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                    assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e = v2.addEdge("friends", v1, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1, Direction.IN);
            os.close();

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.IN, detachedVertex -> {
                            assertEquals(v1.id().toString(), detachedVertex.id().toString());  // lossy
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                            assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                            assertEquals("marko", detachedVertex.value("name"));
                            calledVertex.set(true);
                            return null;
                        },
                        detachedEdge -> {
                            assertEquals(e.id().toString(), detachedEdge.id().toString());  // lossy
                            assertEquals(v1.id().toString(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id().toString());  // lossy
                            assertEquals(v2.id().toString(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id().toString());   // lossy
                            assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                            assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                            assertEquals(e.label(), detachedEdge.label());
                            assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                            assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
    public void shouldReadWriteVertexWithBOTHBOTHEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5d);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge1 = new AtomicBoolean(false);
            final AtomicBoolean calledEdge2 = new AtomicBoolean(false);

            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.BOTH, detachedVertex -> {
                            assertEquals(v1.id(), detachedVertex.id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                            assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                            assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                            calledVertex.set(true);

                            return detachedVertex;
                        },
                        detachedEdge -> {
                            if (detachedEdge.id().equals(e1.id())) {
                                assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                                assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                                assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                                assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                                assertEquals(e1.label(), detachedEdge.label());
                                assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                                assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
                                assertEquals(0.5d, detachedEdge.value("weight"), 0.00001d);
                                calledEdge1.set(true);
                            } else if (detachedEdge.id().equals(e2.id())) {
                                assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                                assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                                assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                                assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                                assertEquals(e1.label(), detachedEdge.label());
                                assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                                assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edge1Called = new AtomicBoolean(false);
            final AtomicBoolean edge2Called = new AtomicBoolean(false);

            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.BOTH, detachedVertex -> {
                            assertEquals(v1.id().toString(), detachedVertex.id().toString());  // lossy
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                            assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                            assertEquals("marko", detachedVertex.value("name"));
                            vertexCalled.set(true);
                            return null;
                        },
                        detachedEdge -> {
                            if (detachedEdge.id().toString().equals(e1.id().toString())) {      // lossy
                                assertEquals(e1.id().toString(), detachedEdge.id().toString());  // lossy
                                assertEquals(v1.id().toString(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id().toString());  // lossy
                                assertEquals(v2.id().toString(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id().toString());   // lossy
                                assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                                assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                                assertEquals(e1.label(), detachedEdge.label());
                                assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                                assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
                                assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);                      // lossy
                                edge1Called.set(true);
                            } else if (detachedEdge.id().toString().equals(e2.id().toString())) { // lossy
                                assertEquals(e2.id().toString(), detachedEdge.id().toString());  // lossy
                                assertEquals(v2.id().toString(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id().toString());  // lossy
                                assertEquals(v1.id().toString(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id().toString());   // lossy
                                assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                                assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                                assertEquals(e2.label(), detachedEdge.label());
                                assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                                assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().embedTypes(true).create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edge1Called = new AtomicBoolean(false);
            final AtomicBoolean edge2Called = new AtomicBoolean(false);

            final GraphSONReader reader = GraphSONReader.build().embedTypes(true).create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.BOTH, detachedVertex -> {
                    assertEquals(v1.id(), detachedVertex.id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                    vertexCalled.set(true);
                    return null;
                }, detachedEdge -> {
                    if (detachedEdge.id().equals(e1.id())) {
                        assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                        assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                        assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                        assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                        assertEquals(e1.label(), detachedEdge.label());
                        assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                        assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
                        assertEquals(0.5f, detachedEdge.value("weight"), 0.00001f);
                        edge1Called.set(true);
                    } else if (detachedEdge.id().equals(e2.id())) {
                        assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                        assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                        assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                        assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                        assertEquals(e1.label(), detachedEdge.label());
                        assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                        assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
    public void shouldReadWriteVertexWithBOTHINEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5d);
        v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edge1Called = new AtomicBoolean(false);

            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.IN, detachedVertex -> {
                            assertEquals(v1.id(), detachedVertex.id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                            assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                            assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                            vertexCalled.set(true);

                            return detachedVertex;
                        },
                        detachedEdge -> {
                            if (detachedEdge.id().equals(e1.id())) {
                                assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                                assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                                assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                                assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                                assertEquals(e1.label(), detachedEdge.label());
                                assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                                assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.IN, detachedVertex -> {
                    assertEquals(v1.id().toString(), detachedVertex.id().toString());  // lossy
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals("marko", detachedVertex.value("name"));
                    vertexCalled.set(true);

                    return null;
                }, detachedEdge -> {
                    if (detachedEdge.id().toString().equals(e1.id().toString())) { // lossy
                        assertEquals(e1.id().toString(), detachedEdge.id().toString());  // lossy
                        assertEquals(v1.id().toString(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id().toString());  // lossy
                        assertEquals(v2.id().toString(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id().toString());   // lossy
                        assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                        assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                        assertEquals(e1.label(), detachedEdge.label());
                        assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                        assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
    public void shouldReadWriteVertexWithBOTHOUTEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = g.addVertex(T.label, "person");
        v2.addEdge("friends", v1, "weight", 0.5d);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.OUT, detachedVertex -> {
                            assertEquals(v1.id(), detachedVertex.id());
                            assertEquals(v1.label(), detachedVertex.label());
                            assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                            assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                            assertEquals(v1.value("name"), detachedVertex.value("name").toString());
                            vertexCalled.set(true);

                            return detachedVertex;
                        },
                        detachedEdge -> {
                            if (detachedEdge.id().equals(e2.id())) {
                                assertEquals(v1.id(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id());
                                assertEquals(v2.id(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id());
                                assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                                assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                                assertEquals(e2.label(), detachedEdge.label());
                                assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                                assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
        final Vertex v1 = g.addVertex("name", "marko", T.label, "person");
        final Vertex v2 = g.addVertex(T.label, "person");
        v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, Direction.OUT, detachedVertex -> {
                    assertEquals(v1.id().toString(), detachedVertex.id().toString());  // lossy
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(0, StreamFactory.stream(detachedVertex.iterators().hiddenPropertyIterator()).count());
                    assertEquals(1, StreamFactory.stream(detachedVertex.iterators().propertyIterator()).count());
                    assertEquals("marko", detachedVertex.value("name"));
                    vertexCalled.set(true);
                    return null;
                }, detachedEdge -> {
                    if (detachedEdge.id().toString().equals(e2.id().toString())) {     // lossy
                        assertEquals(e2.id().toString(), detachedEdge.id().toString());  // lossy
                        assertEquals(v2.id().toString(), detachedEdge.iterators().vertexIterator(Direction.IN).next().id().toString());  // lossy
                        assertEquals(v1.id().toString(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().id().toString());   // lossy
                        assertEquals(v1.label(), detachedEdge.iterators().vertexIterator(Direction.OUT).next().label());
                        assertEquals(v2.label(), detachedEdge.iterators().vertexIterator(Direction.IN).next().label());
                        assertEquals(e2.label(), detachedEdge.label());
                        assertEquals(0, StreamFactory.stream(detachedEdge.iterators().hiddenPropertyIterator()).count());
                        assertEquals(1, StreamFactory.stream(detachedEdge.iterators().propertyIterator()).count());
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
    public void shouldReadWriteVertexWithOUTBOTHEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1, Direction.OUT);

            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
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
    public void shouldReadWriteVertexWithINBOTHEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1, Direction.IN);

            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
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
    public void shouldReadWriteVertexWithINOUTEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1, Direction.IN);

            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
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
    public void shouldReadWriteVertexWithOUTINEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().custom(graphProvider.createConfiguredGremlinKryo()).create();
            writer.writeVertex(os, v1, Direction.IN);

            final KryoReader reader = KryoReader.build()
                    .custom(graphProvider.createConfiguredGremlinKryo())
                    .workingDirectory(File.separator + "tmp").create();
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
        final GraphReader reader = LegacyGraphSONReader.build().build();
        try (final InputStream stream = IoTest.class.getResourceAsStream(GRAPHSON_RESOURCE_PATH_PREFIX + "tinkerpop-classic-legacy.json")) {
            reader.readGraph(stream, g);
        }

        // the id is lossy in migration because TP2 treated ID as String
        assertClassicGraph(g, false, true);
    }

    public static void assertCrewGraph(final Graph g1, final boolean lossyForId) {
        assertEquals(new Long(6), g1.V().count().next());
        assertEquals(new Long(14), g1.E().count().next());

        assertEquals("marko", g1.variables().get("creator").get().toString());
        assertEquals(2014, g1.variables().get("lastModified").get());
        assertEquals("this graph was created to provide examples and test coverage for tinkerpop3 api advances", g1.variables().get("comment").get().toString());

        final Vertex v1 = (Vertex) g1.V().has("name", "marko").next();
        assertEquals("person", v1.label());
        assertEquals(true, v1.iterators().hiddenValueIterator("visible").next());
        assertEquals(2, v1.keys().size());
        assertEquals(1, v1.hiddenKeys().size());
        assertEquals(4, (int) StreamFactory.stream(v1.iterators().propertyIterator("location")).count());
        v1.iterators().propertyIterator("location").forEachRemaining(vp -> {
            if (vp.value().equals("san diego")) {
                assertEquals(1997, (int) vp.value("startTime"));
                assertEquals(2001, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("santa cruz")) {
                assertEquals(2001, (int) vp.value("startTime"));
                assertEquals(2004, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("brussels")) {
                assertEquals(2004, (int) vp.value("startTime"));
                assertEquals(2005, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("santa fe")) {
                assertEquals(2005, (int) vp.value("startTime"));
                assertEquals(1, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else {
                fail("Found a value that should be there");
            }
        });
        assertId(g1, lossyForId, v1, 1);

        final List<Edge> v1Edges = v1.bothE().toList();
        assertEquals(4, v1Edges.size());
        v1Edges.forEach(e -> {
            if (e.inV().values("name").next().equals("gremlin") && e.label().equals("develops")) {
                assertEquals(2009, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 13);
            } else if (e.inV().values("name").next().equals("tinkergraph") && e.label().equals("develops")) {
                assertEquals(2010, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 14);
            } else if (e.inV().values("name").next().equals("gremlin") && e.label().equals("uses")) {
                assertEquals(4, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 15);
            } else if (e.inV().values("name").next().equals("tinkergraph") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 16);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v7 = (Vertex) g1.V().has("name", "stephen").next();
        assertEquals("person", v7.label());
        assertEquals(true, v7.iterators().hiddenValueIterator("visible").next());
        assertEquals(2, v7.keys().size());
        assertEquals(1, v7.hiddenKeys().size());
        assertEquals(3, (int) StreamFactory.stream(v7.iterators().propertyIterator("location")).count());
        v7.iterators().propertyIterator("location").forEachRemaining(vp -> {
            if (vp.value().equals("centreville")) {
                assertEquals(1990, (int) vp.value("startTime"));
                assertEquals(2000, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("dulles")) {
                assertEquals(2000, (int) vp.value("startTime"));
                assertEquals(2006, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("purcellville")) {
                assertEquals(2006, (int) vp.value("startTime"));
                assertEquals(1, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else {
                fail("Found a value that should be there");
            }
        });
        assertId(g1, lossyForId, v7, 7);

        final List<Edge> v7Edges = v7.bothE().toList();
        assertEquals(4, v7Edges.size());
        v7Edges.forEach(e -> {
            if (e.inV().values("name").next().equals("gremlin") && e.label().equals("develops")) {
                assertEquals(2010, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 17);
            } else if (e.inV().values("name").next().equals("tinkergraph") && e.label().equals("develops")) {
                assertEquals(2011, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 18);
            } else if (e.inV().values("name").next().equals("gremlin") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 19);
            } else if (e.inV().values("name").next().equals("tinkergraph") && e.label().equals("uses")) {
                assertEquals(4, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 20);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v8 = (Vertex) g1.V().has("name", "matthias").next();
        assertEquals("person", v8.label());
        assertEquals(true, v8.iterators().hiddenValueIterator("visible").next());
        assertEquals(2, v8.keys().size());
        assertEquals(1, v8.hiddenKeys().size());
        assertEquals(4, (int) StreamFactory.stream(v8.iterators().propertyIterator("location")).count());
        v8.iterators().propertyIterator("location").forEachRemaining(vp -> {
            if (vp.value().equals("bremen")) {
                assertEquals(2004, (int) vp.value("startTime"));
                assertEquals(2007, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("baltimore")) {
                assertEquals(2007, (int) vp.value("startTime"));
                assertEquals(2011, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("oakland")) {
                assertEquals(2011, (int) vp.value("startTime"));
                assertEquals(2014, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("seattle")) {
                assertEquals(2014, (int) vp.value("startTime"));
                assertEquals(1, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else {
                fail("Found a value that should be there");
            }
        });
        assertId(g1, lossyForId, v8, 8);

        final List<Edge> v8Edges = v8.bothE().toList();
        assertEquals(3, v8Edges.size());
        v8Edges.forEach(e -> {
            if (e.inV().values("name").next().equals("gremlin") && e.label().equals("develops")) {
                assertEquals(2012, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 21);
            } else if (e.inV().values("name").next().equals("gremlin") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 22);
            } else if (e.inV().values("name").next().equals("tinkergraph") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 23);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v9 = (Vertex) g1.V().has("name", "daniel").next();
        assertEquals("person", v9.label());
        assertEquals(false, v9.iterators().hiddenValueIterator("visible").next());
        assertEquals(2, v9.keys().size());
        assertEquals(1, v9.hiddenKeys().size());
        assertEquals(3, (int) StreamFactory.stream(v9.iterators().propertyIterator("location")).count());
        v9.iterators().propertyIterator("location").forEachRemaining(vp -> {
            if (vp.value().equals("spremberg")) {
                assertEquals(1982, (int) vp.value("startTime"));
                assertEquals(2005, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("kaiserslautern")) {
                assertEquals(2005, (int) vp.value("startTime"));
                assertEquals(2009, (int) vp.value("endTime"));
                assertEquals(2, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else if (vp.value().equals("aachen")) {
                assertEquals(2009, (int) vp.value("startTime"));
                assertEquals(1, (int) StreamFactory.stream(vp.iterators().propertyIterator()).count());
            } else {
                fail("Found a value that should be there");
            }
        });
        assertId(g1, lossyForId, v9, 9);

        final List<Edge> v9Edges = v9.bothE().toList();
        assertEquals(2, v9Edges.size());
        v9Edges.forEach(e -> {
            if (e.inV().values("name").next().equals("gremlin") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 24);
            } else if (e.inV().values("name").next().equals("tinkergraph") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 25);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v10 = (Vertex) g1.V().has("name", "gremlin").next();
        assertEquals("software", v10.label());
        assertEquals(true, v10.iterators().hiddenValueIterator("visible").next());
        assertEquals(1, v10.keys().size());
        assertEquals(1, v10.hiddenKeys().size());
        assertId(g1, lossyForId, v10, 10);

        final List<Edge> v10Edges = v10.bothE().toList();
        assertEquals(8, v10Edges.size());
        v10Edges.forEach(e -> {
            if (e.outV().values("name").next().equals("marko") && e.label().equals("develops")) {
                assertEquals(2009, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 13);
            } else if (e.outV().values("name").next().equals("marko") && e.label().equals("uses")) {
                assertEquals(4, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 15);
            } else if (e.outV().values("name").next().equals("stephen") && e.label().equals("develops")) {
                assertEquals(2010, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 17);
            } else if (e.outV().values("name").next().equals("stephen") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 19);
            } else if (e.outV().values("name").next().equals("matthias") && e.label().equals("develops")) {
                assertEquals(2012, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 21);
            } else if (e.outV().values("name").next().equals("matthias") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 22);
            } else if (e.outV().values("name").next().equals("daniel") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 24);
            } else if (e.inV().values("name").next().equals("tinkergraph") && e.label().equals("traverses")) {
                assertEquals(false, e.value(Graph.Key.hide("visible")));
                assertEquals(1, e.hiddenKeys().size());
                assertId(g1, lossyForId, e, 26);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v11 = (Vertex) g1.V().has("name", "tinkergraph").next();
        assertEquals("software", v11.label());
        assertEquals(false, v11.iterators().hiddenValueIterator("visible").next());
        assertEquals(1, v11.keys().size());
        assertEquals(1, v11.hiddenKeys().size());
        assertId(g1, lossyForId, v11, 11);

        final List<Edge> v11Edges = v11.bothE().toList();
        assertEquals(7, v11Edges.size());
        v11Edges.forEach(e -> {
            if (e.outV().values("name").next().equals("marko") && e.label().equals("develops")) {
                assertEquals(2010, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 14);
            } else if (e.outV().values("name").next().equals("marko") && e.label().equals("uses")) {
                assertEquals(5, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 16);
            } else if (e.outV().values("name").next().equals("stephen") && e.label().equals("develops")) {
                assertEquals(2011, (int) e.value("since"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 18);
            } else if (e.outV().values("name").next().equals("stephen") && e.label().equals("uses")) {
                assertEquals(4, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 20);
            } else if (e.outV().values("name").next().equals("matthias") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 23);
            } else if (e.outV().values("name").next().equals("daniel") && e.label().equals("uses")) {
                assertEquals(3, (int) e.value("skill"));
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 25);
            } else if (e.outV().values("name").next().equals("gremlin") && e.label().equals("traverses")) {
                assertEquals(false, e.value(Graph.Key.hide("visible")));
                assertEquals(1, e.hiddenKeys().size());
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
        assertEquals(new Long(6), g1.V().count().next());
        assertEquals(new Long(6), g1.E().count().next());

        final Vertex v1 = (Vertex) g1.V().has("name", "marko").next();
        assertEquals(29, v1.<Integer>value("age").intValue());
        assertEquals(2, v1.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v1.label());
        assertId(g1, lossyForId, v1, 1);

        final List<Edge> v1Edges = v1.bothE().toList();
        assertEquals(3, v1Edges.size());
        v1Edges.forEach(e -> {
            if (e.inV().values("name").next().equals("vadas")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(0.5d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.5f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 7);
            } else if (e.inV().values("name").next().equals("josh")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(1.0, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 8);
            } else if (e.inV().values("name").next().equals("lop")) {
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

        final Vertex v2 = (Vertex) g1.V().has("name", "vadas").next();
        assertEquals(27, v2.<Integer>value("age").intValue());
        assertEquals(2, v2.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v2.label());
        assertId(g1, lossyForId, v2, 2);

        final List<Edge> v2Edges = v2.bothE().toList();
        assertEquals(1, v2Edges.size());
        v2Edges.forEach(e -> {
            if (e.outV().values("name").next().equals("marko")) {
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

        final Vertex v3 = (Vertex) g1.V().has("name", "lop").next();
        assertEquals("java", v3.<String>value("lang"));
        assertEquals(2, v2.keys().size());
        assertEquals(assertSpecificLabel ? "software" : Vertex.DEFAULT_LABEL, v3.label());
        assertId(g1, lossyForId, v3, 3);

        final List<Edge> v3Edges = v3.bothE().toList();
        assertEquals(3, v3Edges.size());
        v3Edges.forEach(e -> {
            if (e.outV().values("name").next().equals("peter")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.2d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.2f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 12);
            } else if (e.outV().next().value("name").equals("josh")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (e.outV().values("name").next().equals("marko")) {
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

        final Vertex v4 = (Vertex) g1.V().has("name", "josh").next();
        assertEquals(32, v4.<Integer>value("age").intValue());
        assertEquals(2, v4.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v4.label());
        assertId(g1, lossyForId, v4, 4);

        final List<Edge> v4Edges = v4.bothE().toList();
        assertEquals(3, v4Edges.size());
        v4Edges.forEach(e -> {
            if (e.inV().values("name").next().equals("ripple")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 10);
            } else if (e.inV().values("name").next().equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (e.outV().values("name").next().equals("marko")) {
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

        final Vertex v5 = (Vertex) g1.V().has("name", "ripple").next();
        assertEquals("java", v5.<String>value("lang"));
        assertEquals(2, v5.keys().size());
        assertEquals(assertSpecificLabel ? "software" : Vertex.DEFAULT_LABEL, v5.label());
        assertId(g1, lossyForId, v5, 5);

        final List<Edge> v5Edges = v5.bothE().toList();
        assertEquals(1, v5Edges.size());
        v5Edges.forEach(e -> {
            if (e.outV().values("name").next().equals("josh")) {
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

        final Vertex v6 = (Vertex) g1.V().has("name", "peter").next();
        assertEquals(35, v6.<Integer>value("age").intValue());
        assertEquals(2, v6.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v6.label());
        assertId(g1, lossyForId, v6, 6);

        final List<Edge> v6Edges = v6.bothE().toList();
        assertEquals(1, v6Edges.size());
        v6Edges.forEach(e -> {
            if (e.inV().values("name").next().equals("lop")) {
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
            // required no-arg for kryo serialization
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

        static class CustomIdJacksonSerializer extends StdSerializer<CustomId> {
            public CustomIdJacksonSerializer() {
                super(CustomId.class);
            }

            @Override
            public void serialize(final CustomId customId, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                    throws IOException, JsonGenerationException {
                ser(customId, jsonGenerator, false);
            }

            @Override
            public void serializeWithType(final CustomId customId, final JsonGenerator jsonGenerator,
                                          final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
                ser(customId, jsonGenerator, true);
            }

            private void ser(final CustomId customId, final JsonGenerator jsonGenerator, final boolean includeType) throws IOException {
                jsonGenerator.writeStartObject();

                if (includeType)
                    jsonGenerator.writeStringField(GraphSONTokens.CLASS, CustomId.class.getName());

                jsonGenerator.writeObjectField("cluster", customId.getCluster());
                jsonGenerator.writeObjectField("elementId", customId.getElementId().toString());
                jsonGenerator.writeEndObject();
            }
        }

        static class CustomIdJacksonDeserializer extends StdDeserializer<CustomId> {
            public CustomIdJacksonDeserializer() {
                super(CustomId.class);
            }

            @Override
            public CustomId deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
                String cluster = null;
                UUID id = null;

                while (!jsonParser.getCurrentToken().isStructEnd()) {
                    if (jsonParser.getText().equals("cluster")) {
                        jsonParser.nextToken();
                        cluster = jsonParser.getText();
                    } else if (jsonParser.getText().equals("elementId")) {
                        jsonParser.nextToken();
                        id = UUID.fromString(jsonParser.getText());
                    } else
                        jsonParser.nextToken();
                }

                if (!Optional.ofNullable(cluster).isPresent())
                    throw deserializationContext.mappingException("Could not deserialze CustomId: 'cluster' is required");
                if (!Optional.ofNullable(id).isPresent())
                    throw deserializationContext.mappingException("Could not deserialze CustomId: 'id' is required");

                return new CustomId(cluster, id);
            }
        }
    }
}
