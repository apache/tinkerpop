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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_ANY_IDS;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoTest extends AbstractGremlinTest {

    private static final String GRAPHML_RESOURCE_PATH_PREFIX = "/com/tinkerpop/gremlin/structure/util/io/graphml/";
    private static final String GRAPHSON_RESOURCE_PATH_PREFIX = "/com/tinkerpop/gremlin/structure/util/io/graphson/";

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadGraphML() throws IOException {
        readGraphMLIntoGraph(g);
        assertToyGraph(g, false, true, false);
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
        final Vertex v = g.addVertex(Element.ID, "1");
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
        g.addVertex(Element.ID, new CustomId("vertex", id));
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
        g.addVertex(Element.ID, new CustomId("vertex", UUID.fromString("AF4B5965-B176-4552-B3C1-FBBE2F52C305")));
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

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldMigrateGraphWithFloat() throws Exception {
        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        GraphMigrator.migrateGraph(g, g1);

        assertToyGraph(g1, false, false, false);

        // need to manually close the "g1" instance
        graphProvider.clear(g1, configuration);
    }

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
        assertToyGraph(g1, true, false, true);

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
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            // by making this lossy for float it will assert floats for doubles
            assertToyGraph(g1, true, false, true);

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
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName());
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            assertToyGraph(g1, false, false, false);

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

            assertToyGraph(g1, true, false, false);

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

            assertToyGraph(g1, true, false, true);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteEdgeToKryoUsingFloatProperty() throws Exception {
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5f, Graph.Key.hide("acl"), "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais,
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.id(), edgeId);
                            assertEquals(v1.id(), outId);
                            assertEquals(v2.id(), inId);
                            assertEquals(e.label(), label);
                            assertEquals(e.keys().size() + e.hiddenKeys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5f, properties[1]);
                            assertEquals(Graph.Key.hide("acl"), properties[2]);
                            assertEquals("rw", properties[3]);

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
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5d, Graph.Key.hide("acl"), "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais,
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.id(), edgeId);
                            assertEquals(v1.id(), outId);
                            assertEquals(v2.id(), inId);
                            assertEquals(e.label(), label);
                            assertEquals(e.keys().size() + e.hiddenKeys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5d, properties[1]);
                            assertEquals(Graph.Key.hide("acl"), properties[2]);
                            assertEquals("rw", properties[3]);

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
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5f, Graph.Key.hide("acl"), "rw");

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais,
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.id().toString(), edgeId.toString()); // lossy
                            assertEquals(v1.id().toString(), outId.toString()); // lossy
                            assertEquals(v2.id().toString(), inId.toString());  // lossy
                            assertEquals(e.label(), label);
                            assertEquals(e.keys().size() + e.hiddenKeys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5d, properties[1]); //lossy
                            assertEquals(Graph.Key.hide("acl"), properties[2]);
                            assertEquals("rw", properties[3]);

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
        final Vertex v1 = g.addVertex(Element.ID, 1l);
        final Vertex v2 = g.addVertex(Element.ID, 2l);
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
                reader.readEdge(bais,
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.id(), edgeId);
                            assertEquals(v1.id(), outId);
                            assertEquals(v2.id(), inId);
                            assertEquals(e.label(), label);
                            assertEquals(e.keys().size() + e.hiddenKeys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5f, properties[1]);
                            assertEquals(Graph.Key.hide("acl"), properties[2]);
                            assertEquals("rw", properties[3]);

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
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
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
                reader.readEdge(bais,
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.id(), edgeId);
                            assertEquals(v1.id(), outId);
                            assertEquals(v2.id(), inId);
                            assertEquals(e.label(), label);
                            assertEquals(e.keys().size(), properties.length / 2);
                            assertEquals("uuid", properties[0]);
                            assertEquals(id, properties[1]);

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
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friend", v2, "uuid", id);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais,
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.id(), edgeId);
                            assertEquals(v1.id(), outId);
                            assertEquals(v2.id(), inId);
                            assertEquals(e.label(), label);
                            assertEquals(e.keys().size(), properties.length / 2);
                            assertEquals("uuid", properties[0]);
                            assertEquals(id, properties[1]);

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
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id(), vertexId);
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(2, m.size());
                            assertEquals(v1.value("name"), m.get("name").toString());
                            assertEquals(v1.hiddenMap().next().get("acl"), m.get(Graph.Key.hide("acl")).toString());

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
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id(), vertexId);
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(2, m.size());
                            assertEquals(v1.value("name"), m.get("name").toString());
                            assertEquals(v1.hiddenMap().next().get("acl"), m.get(Graph.Key.hide("acl")).toString());

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
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id().toString(), vertexId.toString()); // lossy
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), ((List) m.get("name")).get(0));

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
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertices(os, g.V().has("age", T.gt, 30));

            final AtomicInteger called = new AtomicInteger(0);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();

            try (final VertexByteArrayInputStream vbais = new VertexByteArrayInputStream(new ByteArrayInputStream(os.toByteArray()))) {
                reader.readVertex(new ByteArrayInputStream(vbais.readVertexBytes().toByteArray()),
                        (vertexId, label, properties) -> {
                            called.incrementAndGet();
                            return mock(Vertex.class);
                        });

                reader.readVertex(new ByteArrayInputStream(vbais.readVertexBytes().toByteArray()),
                        (vertexId, label, properties) -> {
                            called.incrementAndGet();
                            return mock(Vertex.class);
                        });
            }

            assertEquals(2, called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldReadWriteVerticesNoEdgesToKryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertices(os, g.V().has("age", T.gt, 30));

            final AtomicInteger called = new AtomicInteger(0);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();

            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                final Iterator<Vertex> itty = reader.readVertices(bais,
                        null,
                        (vertexId, label, properties) -> {
                            called.incrementAndGet();
                            return mock(Vertex.class);
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
            writer.writeVertices(os, g.V().has("age", T.gt, 30));

            final AtomicInteger called = new AtomicInteger(0);
            final GraphSONReader reader = GraphSONReader.build().create();
            final BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(os.toByteArray())));
            String line = br.readLine();
            reader.readVertex(new ByteArrayInputStream(line.getBytes()),
                    (vertexId, label, properties) -> {
                        called.incrementAndGet();
                        return mock(Vertex.class);
                    });

            line = br.readLine();
            reader.readVertex(new ByteArrayInputStream(line.getBytes()),
                    (vertexId, label, properties) -> {
                        called.incrementAndGet();
                        return mock(Vertex.class);
                    });

            assertEquals(2, called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldReadWriteVerticesNoEdgesToGraphSON() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertices(os, g.V().has("age", T.gt, 30));

            final AtomicInteger called = new AtomicInteger(0);
            final GraphSONReader reader = GraphSONReader.build().create();
            final Iterator<Vertex> itty = reader.readVertices(new ByteArrayInputStream(os.toByteArray()), null,
                    (vertexId, label, properties) -> {
                        called.incrementAndGet();
                        return mock(Vertex.class);
                    }, null);

            assertNotNull(itty.next());
            assertNotNull(itty.next());
            assertFalse(itty.hasNext());
            assertEquals(2, called.get());
        }
    }


    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithOUTOUTEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");

        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1, Direction.OUT);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id(), vertexId);
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), m.get("name").toString());

                            calledVertex.set(true);
                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.id()).thenReturn(v1.id());
                            return vsub1;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.id(), edgeId);
                            assertEquals(v1.id(), outId);
                            assertEquals(v2.id(), inId);
                            assertEquals(e.label(), label);
                            assertEquals(e.keys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5d, properties[1]);

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
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithOUTOUTEdgesToGraphSON() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1, Direction.OUT);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id().toString(), vertexId.toString());  // lossy
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), ((List) m.get("name")).get(0));
                            calledVertex.set(true);
                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.id().toString(), edgeId.toString());  // lossy
                            assertEquals(v1.id().toString(), outId.toString());  // lossy
                            assertEquals(v2.id().toString(), inId.toString());   // lossy
                            assertEquals(e.label(), label);
                            assertEquals(e.keys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5d, properties[1]);                      // lossy

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
        final Vertex v1 = g.addVertex("name", "marko");

        final Vertex v2 = g.addVertex();
        final Edge e = v2.addEdge("friends", v1, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1, Direction.IN);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);

            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.IN,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id(), vertexId);
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), m.get("name").toString());

                            calledVertex.set(true);

                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.id()).thenReturn(v1.id());
                            return vsub1;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.id(), edgeId);
                            assertEquals(v2.id(), outId);
                            assertEquals(v1.id(), inId);
                            assertEquals(e.label(), label);
                            assertEquals(e.keys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5d, properties[1]);

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
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithININEdgesToGraphSON() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        final Edge e = v2.addEdge("friends", v1, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1, Direction.IN);
            os.close();

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.IN,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id().toString(), vertexId.toString()); // lossy
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), ((List) m.get("name")).get(0));
                            calledVertex.set(true);
                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.id().toString(), edgeId.toString()); // lossy
                            assertEquals(v1.id().toString(), inId.toString());  // lossy
                            assertEquals(v2.id().toString(), outId.toString()); // lossy
                            assertEquals(e.label(), label);
                            assertEquals(e.keys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5d, properties[1]);                     // lossy

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
        final Vertex v1 = g.addVertex("name", "marko");

        final Vertex v2 = g.addVertex();
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5d);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge1 = new AtomicBoolean(false);
            final AtomicBoolean calledEdge2 = new AtomicBoolean(false);

            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.BOTH,
                        (vertexId, label, properties) -> {
                            if (g.features().vertex().supportsUserSuppliedIds())
                                assertEquals(v1.id(), vertexId);

                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), m.get("name").toString());

                            calledVertex.set(true);

                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.id()).thenReturn(v1.id());
                            return vsub1;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.equals(e1.id())) {
                                assertEquals(v2.id(), outId);
                                assertEquals(v1.id(), inId);
                                assertEquals(e1.label(), label);
                                assertEquals(e1.keys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(0.5d, properties[1]);

                                calledEdge1.set(true);
                            } else if (edgeId.equals(e2.id())) {
                                assertEquals(v1.id(), outId);
                                assertEquals(v2.id(), inId);
                                assertEquals(e2.label(), label);
                                assertEquals(e2.keys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(1.0d, properties[1]);

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
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHBOTHEdgesToKryoSkipProperties() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");

        final Vertex v2 = g.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5d);
        v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        (vertexId, label, properties) -> {
                            if (g.features().vertex().supportsUserSuppliedIds())
                                assertEquals(v1.id(), vertexId);

                            assertEquals(v1.label(), label);

                            calledVertex.set(true);

                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.id()).thenReturn(v1.id());
                            return vsub1;
                        });
            }

            assertTrue(calledVertex.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHBOTHEdgesToGraphSON() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
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
                reader.readVertex(bais,
                        Direction.BOTH,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id().toString(), vertexId.toString());  // lossy
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), ((List) m.get("name")).get(0));

                            vertexCalled.set(true);

                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.toString().equals(e1.id().toString())) {      // lossy
                                assertEquals(v2.id().toString(), outId.toString());  // lossy
                                assertEquals(v1.id().toString(), inId.toString());   // lossy
                                assertEquals(e1.label(), label);
                                assertEquals(e1.keys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(0.5d, properties[1]);                      // lossy

                                edge1Called.set(true);
                            } else if (edgeId.toString().equals(e2.id().toString())) { // lossy
                                assertEquals(v1.id().toString(), outId.toString());    // lossy
                                assertEquals(v2.id().toString(), inId.toString());     // lossy
                                assertEquals(e2.label(), label);
                                assertEquals(e2.keys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(1.0d, properties[1]);                        // lossy

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
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
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
                reader.readVertex(bais,
                        Direction.BOTH,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id(), vertexId);
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), ((List) m.get("name")).get(0));

                            vertexCalled.set(true);

                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.equals(e1.id())) {
                                assertEquals(v2.id(), outId);
                                assertEquals(v1.id(), inId);
                                assertEquals(e1.label(), label);
                                assertEquals(e1.keys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(0.5f, properties[1]);

                                edge1Called.set(true);
                            } else if (edgeId.equals(e2.id())) {
                                assertEquals(v1.id(), outId);
                                assertEquals(v2.id(), inId);
                                assertEquals(e2.label(), label);
                                assertEquals(e2.keys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(1.0f, properties[1]);

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
        final Vertex v1 = g.addVertex("name", "marko");

        final Vertex v2 = g.addVertex();
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5d);
        v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edge1Called = new AtomicBoolean(false);

            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.IN,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id(), vertexId);
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), m.get("name").toString());

                            vertexCalled.set(true);

                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.id()).thenReturn(v1.id());
                            return vsub1;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.equals(e1.id())) {
                                assertEquals(v2.id(), outId);
                                assertEquals(v1.id(), inId);
                                assertEquals(e1.label(), label);
                                assertEquals(e1.keys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(0.5d, properties[1]);

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
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.IN,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id().toString(), vertexId.toString()); // lossy
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), ((List) m.get("name")).get(0));

                            vertexCalled.set(true);

                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.toString().equals(e1.id().toString())) { // lossy
                                assertEquals(v2.id().toString(), outId.toString()); // lossy
                                assertEquals(v1.id().toString(), inId.toString()); // lossy
                                assertEquals(e1.label(), label);
                                assertEquals(e1.keys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(0.5d, properties[1]);                    // lossy

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
        final Vertex v1 = g.addVertex("name", "marko");

        final Vertex v2 = g.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5d);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id(), vertexId);
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), m.get("name").toString());

                            vertexCalled.set(true);

                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.id()).thenReturn(v1.id());
                            return vsub1;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.equals(e2.id())) {
                                assertEquals(v1.id(), outId);
                                assertEquals(v2.id(), inId);
                                assertEquals(e2.label(), label);
                                assertEquals(e2.keys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(1.0d, properties[1]);

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
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = GraphSONWriter.build().create();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final GraphSONReader reader = GraphSONReader.build().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.id().toString(), vertexId.toString());  // lossy
                            assertEquals(v1.label(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.value("name"), ((List) m.get("name")).get(0));

                            vertexCalled.set(true);

                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.toString().equals(e2.id().toString())) {     // lossy
                                assertEquals(v1.id().toString(), outId.toString()); // lossy
                                assertEquals(v2.id().toString(), inId.toString()); // lossy
                                assertEquals(e2.label(), label);
                                assertEquals(e2.keys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(1.0d, properties[1]);                 // lossy

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
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1, Direction.OUT);

            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.BOTH,
                        (vertexId, label, properties) -> null,
                        (edgeId, outId, inId, label, properties) -> null);
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
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1, Direction.IN);

            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.BOTH,
                        (vertexId, label, properties) -> null,
                        (edgeId, outId, inId, label, properties) -> null);
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
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1, Direction.IN);

            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> null,
                        (edgeId, outId, inId, label, properties) -> null);
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
            final KryoWriter writer = KryoWriter.build().create();
            writer.writeVertex(os, v1, Direction.IN);

            final KryoReader reader = KryoReader.build()
                    .setWorkingDirectory(File.separator + "tmp").create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> null,
                        (edgeId, outId, inId, label, properties) -> null);
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
        assertToyGraph(g, false, true, false);
    }

    public static void assertToyGraph(final Graph g1, final boolean assertDouble, final boolean lossyForId, final boolean assertSpecificLabel) {
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
            if (e.inV().value("name").next().equals("vadas")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(0.5d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.5f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 7);
            } else if (e.inV().value("name").next().equals("josh")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertEquals(1.0, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 8);
            } else if (e.inV().value("name").next().equals("lop")) {
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
            if (e.outV().value("name").next().equals("marko")) {
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
            if (e.outV().value("name").next().equals("peter")) {
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
            } else if (e.outV().value("name").next().equals("marko")) {
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
            if (e.inV().value("name").next().equals("ripple")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(1.0d, e.value("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 10);
            } else if (e.inV().value("name").next().equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertEquals(0.4d, e.value("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.value("weight"), 0.0001f);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (e.outV().value("name").next().equals("marko")) {
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
            if (e.outV().value("name").next().equals("josh")) {
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
            if (e.inV().value("name").next().equals("lop")) {
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

    private static void readGraphSONIntoGraph(final Graph g) throws IOException {
        final GraphReader reader = GraphSONReader.build().embedTypes(true).create();
        try (final InputStream stream = IoTest.class.getResourceAsStream(GRAPHSON_RESOURCE_PATH_PREFIX + "tinkerpop-classic-typed.json")) {
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
