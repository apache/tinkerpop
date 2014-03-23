package com.tinkerpop.gremlin.structure;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import com.tinkerpop.gremlin.structure.io.GraphMigrator;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.GraphWriter;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import org.apache.commons.configuration.Configuration;
import org.junit.Ignore;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.FEATURE_STRING_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoTest extends AbstractGremlinTest {

    // todo: should expand test here significantly.  see blueprints2

    private static final String GRAPHML_RESOURCE_PATH_PREFIX = "/com/tinkerpop/gremlin/structure/util/io/graphml/";
    private static final String GRAPHSON_RESOURCE_PATH_PREFIX = "/com/tinkerpop/gremlin/structure/util/io/graphson/";

    @Ignore("sort out ID issues now that tinkergraph keeps it as object")
    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadGraphML() throws IOException {
        readGraphMLIntoGraph(g);
        assertClassicGraph(g, false);
    }

    /**
     * Only need to execute this test with TinkerGraph or other graphs that support user supplied identifiers.
     */
    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldWriteNormalizedGraphML() throws Exception {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            final GraphMLWriter w = new GraphMLWriter.Builder().setNormalize(true).build();
            w.writeGraph(bos, g);

            final String expected = streamToString(IoTest.class.getResourceAsStream(GRAPHML_RESOURCE_PATH_PREFIX + "graph-example-1-normalized.xml"));
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
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldWriteNormalizedGraphMLWithEdgeLabel() throws Exception {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            final GraphMLWriter w = new GraphMLWriter.Builder()
                    .setNormalize(true)
                    .edgeLabelKey("label").build();
            w.writeGraph(bos, g);

            String expected = streamToString(IoTest.class.getResourceAsStream(GRAPHML_RESOURCE_PATH_PREFIX + "graph-example-1-schema-valid.xml"));
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
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldProperlyEncodeWithGraphML() throws Exception {
        final Vertex v = g.addVertex(Element.ID, "1");
        v.setProperty("text", "\u00E9");

        final GraphMLWriter w = new GraphMLWriter.Builder().build();

        final File f = File.createTempFile("test", "txt");
        try (final OutputStream out = new FileOutputStream(f)) {
            w.writeGraph(out, g);
        }

        validateXmlAgainstGraphMLXsd(f);

        // reusing the same config used for creation of "g".
        final Configuration configuration = graphProvider.newGraphConfiguration("g2");
        graphProvider.clear(null, configuration);
        final Graph g2 = graphProvider.openTestGraph(configuration);
        final GraphMLReader r = new GraphMLReader.Builder().build();

        try (final InputStream in = new FileInputStream(f)) {
            r.readGraph(in, g2);
        }

        final Vertex v2 = g2.v("1");
        assertEquals("\u00E9", v2.getProperty("text").get());

        // need to manually close the "g2" instance
        graphProvider.clear(g2, configuration);
    }

    @Ignore("sort out ID issues now that tinkergraph keeps it as object")
    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void shouldProperlySerializeCustomIdWithGraphSON() throws Exception {
        final Vertex v = g.addVertex(Element.ID, new CustomId("vertex", UUID.fromString("AF4B5965-B176-4552-B3C1-FBBE2F52C305")));
        final SimpleModule module = new SimpleModule();
        module.addSerializer(CustomId.class, new CustomId.CustomIdJacksonSerializer());
        final GraphWriter writer = new GraphSONWriter.Builder().customSerializer(module).build();

        // todo: finish off these tests
        //writer.writeGraph(System.out, g);
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldMigrateGraph() throws Exception {
        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph");
        graphProvider.clear(null, configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        GraphMigrator.migrateGraph(g, g1);

        assertClassicGraph(g1, false);

        // need to manually close the "g1" instance
        graphProvider.clear(g1, configuration);
    }


    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldReadWriteClassicToKryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph");
            graphProvider.clear(null, configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            assertClassicGraph(g1, false);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldReadWriteClassicToGraphSON() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = new GraphSONWriter.Builder().build();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph");
            graphProvider.clear(null, configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GraphSONReader reader = new GraphSONReader.Builder().build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            assertClassicGraph(g1, true);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexAnnotationFeatures.class, feature = Graph.Features.VertexAnnotationFeatures.FEATURE_ANNOTATIONS)
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldReadWriteModernToKryo() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeGraph(os, g);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph");
            graphProvider.clear(null, configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            assertModernGraph(g1);

            // need to manually close the "g1" instance
            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteEdgeToKryo() throws Exception {
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais,
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.getId(), edgeId);
                            assertEquals(v1.getId(), outId);
                            assertEquals(v2.getId(), inId);
                            assertEquals(e.getLabel(), label);
                            assertEquals(e.getPropertyKeys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5f, properties[1]);

                            called.set(true);

                            return null;
                        });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteEdgeToGraphSON() throws Exception {
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = new GraphSONWriter.Builder().build();
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = new GraphSONReader.Builder().build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais,
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.getId().toString(), edgeId.toString()); // lossy
                            assertEquals(v1.getId().toString(), outId.toString()); // lossy
                            assertEquals(v2.getId().toString(), inId.toString());  // lossy
                            assertEquals(e.getLabel(), label);
                            assertEquals(e.getPropertyKeys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5d, properties[1]);    // lossy

                            called.set(true);

                            return null;
                        });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexAnnotationFeatures.class, feature = Graph.Features.VertexAnnotationFeatures.FEATURE_ANNOTATIONS)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexNoEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", "locations", AnnotatedList.make());
        final AnnotatedList<String> locations = v1.getValue("locations");
        locations.addValue("san diego", "startTime", 1997, "endTime", 2001);
        locations.addValue("santa cruz", "startTime", 2001, "endTime", 2004);

        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeVertex(os, v1);

            final AnnotatedList locationAnnotatedList = mock(AnnotatedList.class);

            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId(), vertexId);
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(2, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());
                            assertEquals(AnnotatedList.make(), m.get("locations"));

                            // return a mock Vertex here so that the annotated list can be tested.  annotated lists are
                            // set after the fact.
                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.getValue("locations")).thenReturn(locationAnnotatedList);
                            return vsub1;
                        });
            }

            verify(locationAnnotatedList).addValue("san diego", "startTime", 1997, "endTime", 2001);
            verify(locationAnnotatedList).addValue("santa cruz", "startTime", 2001, "endTime", 2004);
        }
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexNoEdgesToGraphSON() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = new GraphSONWriter.Builder().build();
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphSONReader reader = new GraphSONReader.Builder().build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId().toString(), vertexId.toString()); // lossy
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());

                            called.set(true);
                            return null;
                        });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexAnnotationFeatures.class, feature = Graph.Features.VertexAnnotationFeatures.FEATURE_ANNOTATIONS)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexWithOUTOUTEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", "locations", AnnotatedList.make());
        final AnnotatedList<String> locations = v1.getValue("locations");
        locations.addValue("san diego", "startTime", 1997, "endTime", 2001);
        locations.addValue("santa cruz", "startTime", 2001, "endTime", 2004);

        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.OUT);

            final AnnotatedList locationAnnotatedList = mock(AnnotatedList.class);

            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId(), vertexId);
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(2, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());
                            assertEquals(AnnotatedList.make(), m.get("locations"));

                            // return a mock Vertex here so that the annotated list can be tested.  annotated lists are
                            // set after the fact.
                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.getValue("locations")).thenReturn(locationAnnotatedList);
                            when(vsub1.getId()).thenReturn(v1.getId());
                            return vsub1;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.getId(), edgeId);
                            assertEquals(v1.getId(), outId);
                            assertEquals(v2.getId(), inId);
                            assertEquals(e.getLabel(), label);
                            assertEquals(e.getPropertyKeys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5f, properties[1]);

                            return null;
                        });
                }

            verify(locationAnnotatedList).addValue("san diego", "startTime", 1997, "endTime", 2001);
            verify(locationAnnotatedList).addValue("santa cruz", "startTime", 2001, "endTime", 2004);

            }
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithOUTOUTEdgesToGraphSON() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = new GraphSONWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.OUT);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final GraphSONReader reader = new GraphSONReader.Builder().build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId().toString(), vertexId.toString());  // lossy
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());
                            calledVertex.set(true);
                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.getId().toString(), edgeId.toString());  // lossy
                            assertEquals(v1.getId().toString(), outId.toString());  // lossy
                            assertEquals(v2.getId().toString(), inId.toString());   // lossy
                            assertEquals(e.getLabel(), label);
                            assertEquals(e.getPropertyKeys().size(), properties.length / 2);
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
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexAnnotationFeatures.class, feature = Graph.Features.VertexAnnotationFeatures.FEATURE_ANNOTATIONS)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexWithININEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", "locations", AnnotatedList.make());
        final AnnotatedList<String> locations = v1.getValue("locations");
        locations.addValue("san diego", "startTime", 1997, "endTime", 2001);
        locations.addValue("santa cruz", "startTime", 2001, "endTime", 2004);

        final Vertex v2 = g.addVertex();
        final Edge e = v2.addEdge("friends", v1, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.IN);

            final AnnotatedList locationAnnotatedList = mock(AnnotatedList.class);

            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.IN,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId(), vertexId);
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(2, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());
                            assertEquals(AnnotatedList.make(), m.get("locations"));

                            // return a mock Vertex here so that the annotated list can be tested.  annotated lists are
                            // set after the fact.
                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.getValue("locations")).thenReturn(locationAnnotatedList);
                            when(vsub1.getId()).thenReturn(v1.getId());
                            return vsub1;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.getId(), edgeId);
                            assertEquals(v2.getId(), outId);
                            assertEquals(v1.getId(), inId);
                            assertEquals(e.getLabel(), label);
                            assertEquals(e.getPropertyKeys().size(), properties.length / 2);
                            assertEquals("weight", properties[0]);
                            assertEquals(0.5f, properties[1]);

                            return null;
                        });
            }

            verify(locationAnnotatedList).addValue("san diego", "startTime", 1997, "endTime", 2001);
            verify(locationAnnotatedList).addValue("santa cruz", "startTime", 2001, "endTime", 2004);
        }
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithININEdgesToGraphSON() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        final Edge e = v2.addEdge("friends", v1, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = new GraphSONWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.IN);
            os.close();

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final GraphSONReader reader = new GraphSONReader.Builder().build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.IN,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId().toString(), vertexId.toString()); // lossy
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());
                            calledVertex.set(true);
                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            assertEquals(e.getId().toString(), edgeId.toString()); // lossy
                            assertEquals(v1.getId().toString(), inId.toString());  // lossy
                            assertEquals(v2.getId().toString(), outId.toString()); // lossy
                            assertEquals(e.getLabel(), label);
                            assertEquals(e.getPropertyKeys().size(), properties.length / 2);
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
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexAnnotationFeatures.class, feature = Graph.Features.VertexAnnotationFeatures.FEATURE_ANNOTATIONS)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexWithBOTHBOTHEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", "locations", AnnotatedList.make());
        final AnnotatedList<String> locations = v1.getValue("locations");
        locations.addValue("san diego", "startTime", 1997, "endTime", 2001);
        locations.addValue("santa cruz", "startTime", 2001, "endTime", 2004);

        final Vertex v2 = g.addVertex();
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AnnotatedList locationAnnotatedList = mock(AnnotatedList.class);

            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.BOTH,
                        (vertexId, label, properties) -> {
                            if (g.getFeatures().vertex().supportsUserSuppliedIds())
                                assertEquals(v1.getId(), vertexId);

                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(2, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());
                            assertEquals(AnnotatedList.make(), m.get("locations"));

                            // return a mock Vertex here so that the annotated list can be tested.  annotated lists are
                            // set after the fact.
                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.getValue("locations")).thenReturn(locationAnnotatedList);
                            when(vsub1.getId()).thenReturn(v1.getId());
                            return vsub1;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.equals(e1.getId())) {
                                assertEquals(v2.getId(), outId);
                                assertEquals(v1.getId(), inId);
                                assertEquals(e1.getLabel(), label);
                                assertEquals(e1.getPropertyKeys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(0.5f, properties[1]);
                            } else if (edgeId.equals(e2.getId())) {
                                assertEquals(v1.getId(), outId);
                                assertEquals(v2.getId(), inId);
                                assertEquals(e2.getLabel(), label);
                                assertEquals(e2.getPropertyKeys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(1.0f, properties[1]);
                            } else {
                                fail("An edge id generated that does not exist");
                            }

                            return null;
                        });
            }

            verify(locationAnnotatedList).addValue("san diego", "startTime", 1997, "endTime", 2001);
            verify(locationAnnotatedList).addValue("santa cruz", "startTime", 2001, "endTime", 2004);
        }
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHBOTHEdgesToGraphSON() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = new GraphSONWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edge1Called = new AtomicBoolean(false);
            final AtomicBoolean edge2Called = new AtomicBoolean(false);

            final GraphSONReader reader = new GraphSONReader.Builder().build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.BOTH,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId().toString(), vertexId.toString());  // lossy
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());

                            vertexCalled.set(true);

                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.toString().equals(e1.getId().toString())) {      // lossy
                                assertEquals(v2.getId().toString(), outId.toString());  // lossy
                                assertEquals(v1.getId().toString(), inId.toString());   // lossy
                                assertEquals(e1.getLabel(), label);
                                assertEquals(e1.getPropertyKeys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(0.5d, properties[1]);                      // lossy

                                edge1Called.set(true);
                            } else if (edgeId.toString().equals(e2.getId().toString())) { // lossy
                                assertEquals(v1.getId().toString(), outId.toString());    // lossy
                                assertEquals(v2.getId().toString(), inId.toString());     // lossy
                                assertEquals(e2.getLabel(), label);
                                assertEquals(e2.getPropertyKeys().size(), properties.length / 2);
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
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexAnnotationFeatures.class, feature = Graph.Features.VertexAnnotationFeatures.FEATURE_ANNOTATIONS)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexWithBOTHINEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", "locations", AnnotatedList.make());
        final AnnotatedList<String> locations = v1.getValue("locations");
        locations.addValue("san diego", "startTime", 1997, "endTime", 2001);
        locations.addValue("santa cruz", "startTime", 2001, "endTime", 2004);

        final Vertex v2 = g.addVertex();
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AnnotatedList locationAnnotatedList = mock(AnnotatedList.class);

            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.IN,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId(), vertexId);
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(2, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());
                            assertEquals(AnnotatedList.make(), m.get("locations"));

                            // return a mock Vertex here so that the annotated list can be tested.  annotated lists are
                            // set after the fact.
                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.getValue("locations")).thenReturn(locationAnnotatedList);
                            when(vsub1.getId()).thenReturn(v1.getId());
                            return vsub1;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.equals(e1.getId())) {
                                assertEquals(v2.getId(), outId);
                                assertEquals(v1.getId(), inId);
                                assertEquals(e1.getLabel(), label);
                                assertEquals(e1.getPropertyKeys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(0.5f, properties[1]);
                            } else {
                                fail("An edge id generated that does not exist");
                            }

                            return null;
                        });
            }

            verify(locationAnnotatedList).addValue("san diego", "startTime", 1997, "endTime", 2001);
            verify(locationAnnotatedList).addValue("santa cruz", "startTime", 2001, "endTime", 2004);
        }
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHINEdgesToGraphSON() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5f);
        v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = new GraphSONWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final GraphSONReader reader = new GraphSONReader.Builder().build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.IN,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId().toString(), vertexId.toString()); // lossy
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());

                            vertexCalled.set(true);

                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.toString().equals(e1.getId().toString())) { // lossy
                                assertEquals(v2.getId().toString(), outId.toString()); // lossy
                                assertEquals(v1.getId().toString(), inId.toString()); // lossy
                                assertEquals(e1.getLabel(), label);
                                assertEquals(e1.getPropertyKeys().size(), properties.length / 2);
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
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexAnnotationFeatures.class, feature = Graph.Features.VertexAnnotationFeatures.FEATURE_ANNOTATIONS)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexWithBOTHOUTEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko", "locations", AnnotatedList.make());
        final AnnotatedList<String> locations = v1.getValue("locations");
        locations.addValue("san diego", "startTime", 1997, "endTime", 2001);
        locations.addValue("santa cruz", "startTime", 2001, "endTime", 2004);

        final Vertex v2 = g.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AnnotatedList locationAnnotatedList = mock(AnnotatedList.class);

            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId(), vertexId);
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(2, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());
                            assertEquals(AnnotatedList.make(), m.get("locations"));

                            // return a mock Vertex here so that the annotated list can be tested.  annotated lists are
                            // set after the fact.
                            final Vertex vsub1 = mock(Vertex.class);
                            when(vsub1.getValue("locations")).thenReturn(locationAnnotatedList);
                            when(vsub1.getId()).thenReturn(v1.getId());
                            return vsub1;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.equals(e2.getId())) {
                                assertEquals(v1.getId(), outId);
                                assertEquals(v2.getId(), inId);
                                assertEquals(e2.getLabel(), label);
                                assertEquals(e2.getPropertyKeys().size(), properties.length / 2);
                                assertEquals("weight", properties[0]);
                                assertEquals(1.0f, properties[1]);
                            } else {
                                fail("An edge id generated that does not exist");
                            }

                            return null;
                        });
            }

            verify(locationAnnotatedList).addValue("san diego", "startTime", 1997, "endTime", 2001);
            verify(locationAnnotatedList).addValue("santa cruz", "startTime", 2001, "endTime", 2004);
        }
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHOUTEdgesToGraphSON() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5f);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphSONWriter writer = new GraphSONWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean vertexCalled = new AtomicBoolean(false);
            final AtomicBoolean edgeCalled = new AtomicBoolean(false);

            final GraphSONReader reader = new GraphSONReader.Builder().build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> {
                            assertEquals(v1.getId().toString(), vertexId.toString());  // lossy
                            assertEquals(v1.getLabel(), label);

                            final Map<String, Object> m = new HashMap<>();
                            for (int i = 0; i < properties.length; i = i + 2) {
                                if (!properties[i].equals(Element.ID))
                                    m.put((String) properties[i], properties[i + 1]);
                            }

                            assertEquals(1, m.size());
                            assertEquals(v1.getValue("name"), m.get("name").toString());

                            vertexCalled.set(true);

                            return null;
                        },
                        (edgeId, outId, inId, label, properties) -> {
                            if (edgeId.toString().equals(e2.getId().toString())) {     // lossy
                                assertEquals(v1.getId().toString(), outId.toString()); // lossy
                                assertEquals(v2.getId().toString(), inId.toString()); // lossy
                                assertEquals(e2.getLabel(), label);
                                assertEquals(e2.getPropertyKeys().size(), properties.length / 2);
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
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexWithOUTBOTHEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.OUT);

        final KryoReader reader = new KryoReader.Builder()
                .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.BOTH,
                        (vertexId, label, properties) -> null,
                        (edgeId, outId, inId, label, properties) -> null);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexWithINBOTHEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.IN);

            final KryoReader reader = new KryoReader.Builder()
                .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.BOTH,
                        (vertexId, label, properties) -> null,
                        (edgeId, outId, inId, label, properties) -> null);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexWithINOUTEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v2.addEdge("friends", v1, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.IN);

            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> null,
                        (edgeId, outId, inId, label, properties) -> null);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    public void shouldReadWriteVertexWithOUTINEdgesToKryo() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Vertex v2 = g.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5f);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final KryoWriter writer = new KryoWriter.Builder().build();
            writer.writeVertex(os, v1, Direction.IN);

            final KryoReader reader = new KryoReader.Builder()
                    .setWorkingDirectory(File.separator + "tmp").build();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais,
                        Direction.OUT,
                        (vertexId, label, properties) -> null,
                        (edgeId, outId, inId, label, properties) -> null);
            }
        }
    }

    public static void assertModernGraph(final Graph g1) {
        if (g1.getFeatures().graph().supportsMemory()) {
            final Map<String,Object> m = g1.memory().asMap();
            if (g1.getFeatures().graph().memory().supportsStringValues())
                assertEquals("modern", m.get("name"));
            if (g1.getFeatures().graph().memory().supportsIntegerValues())
                assertEquals(2014, m.get("year"));
        }

        assertEquals(6, g1.V().count());
        assertEquals(8, g1.E().count());

        final Vertex v1 = (Vertex) g1.V().has("name", "marko").next();
        assertEquals("person", v1.getLabel());
        assertEquals(2, v1.getPropertyKeys().size());
        if (g1.getFeatures().vertex().supportsUserSuppliedIds())
            assertEquals(1, v1.getId());
        final AnnotatedList<String> v1location = v1.getValue("locations");
        assertEquals(4, v1location.annotatedValues().toList().size());
        v1location.annotatedValues().toList().forEach(av -> {
            if (av.getValue().equals("san diego")) {
                assertEquals(1997, av.getAnnotation("startTime").get());
                assertEquals(2001, av.getAnnotation("endTime").get());
            } else if (av.getValue().equals("santa cruz")) {
                assertEquals(2001, av.getAnnotation("startTime").get());
                assertEquals(2004, av.getAnnotation("endTime").get());
            } else if (av.getValue().equals("brussels")) {
                assertEquals(2004, av.getAnnotation("startTime").get());
                assertEquals(2005, av.getAnnotation("endTime").get());
            } else if (av.getValue().equals("santa fe")) {
                assertEquals(2005, av.getAnnotation("startTime").get());
                assertEquals(2014, av.getAnnotation("endTime").get());
            }

            assertEquals(2, av.getAnnotationKeys().size());
        });

    }

    public static void assertClassicGraph(final Graph g1, final boolean lossyOfFloat) {
        assertEquals(6, g1.V().count());
        assertEquals(6, g1.E().count());

        final Vertex v1 = (Vertex) g1.V().has("name", "marko").next();
        assertEquals(29, v1.<Integer>getValue("age").intValue());
        assertEquals(2, v1.getPropertyKeys().size());
        if (g1.getFeatures().vertex().supportsUserSuppliedIds())
            assertEquals(1, v1.getId());

        final List<Edge> v1Edges = v1.bothE().toList();
        assertEquals(3, v1Edges.size());
        v1Edges.forEach(e -> {
            if (e.getVertex(Direction.IN).getValue("name").equals("vadas")) {
                assertEquals("knows", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(0.5d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(0.5f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(7, e.getId());
            } else if (e.getVertex(Direction.IN).getValue("name").equals("josh")) {
                assertEquals("knows", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(1.0, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(8, e.getId());
            } else if (e.getVertex(Direction.IN).getValue("name").equals("lop")) {
                assertEquals("created", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(0.4d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(9, e.getId());
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v2 = (Vertex) g1.V().has("name", "vadas").next();
        assertEquals(27, v2.<Integer>getValue("age").intValue());
        assertEquals(2, v2.getPropertyKeys().size());
        if (g1.getFeatures().vertex().supportsUserSuppliedIds())
            assertEquals(2, v2.getId());

        final List<Edge> v2Edges = v2.bothE().toList();
        assertEquals(1, v2Edges.size());
        v2Edges.forEach(e -> {
            if (e.getVertex(Direction.OUT).getValue("name").equals("marko")) {
                assertEquals("knows", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(0.5d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(0.5f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(7, e.getId());
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v3 = (Vertex) g1.V().has("name", "lop").next();
        assertEquals("java", v3.<String>getValue("lang"));
        assertEquals(2, v2.getPropertyKeys().size());
        if (g1.getFeatures().vertex().supportsUserSuppliedIds())
            assertEquals(3, v3.getId());

        final List<Edge> v3Edges = v3.bothE().toList();
        assertEquals(3, v3Edges.size());
        v3Edges.forEach(e -> {
            if (e.getVertex(Direction.OUT).getValue("name").equals("peter")) {
                assertEquals("created", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(0.2d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(0.2f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(12, e.getId());
            } else if (e.getVertex(Direction.OUT).getValue("name").equals("josh")) {
                assertEquals("created", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(0.4d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(11, e.getId());
            } else if (e.getVertex(Direction.OUT).getValue("name").equals("marko")) {
                assertEquals("created", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(0.4d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(9, e.getId());
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v4 = (Vertex) g1.V().has("name", "josh").next();
        assertEquals(32, v4.<Integer>getValue("age").intValue());
        assertEquals(2, v4.getPropertyKeys().size());
        if (g1.getFeatures().vertex().supportsUserSuppliedIds())
            assertEquals(4, v4.getId());

        final List<Edge> v4Edges = v4.bothE().toList();
        assertEquals(3, v4Edges.size());
        v4Edges.forEach(e -> {
            if (e.getVertex(Direction.IN).getValue("name").equals("ripple")) {
                assertEquals("created", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(1.0d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(10, e.getId());
            } else if (e.getVertex(Direction.IN).getValue("name").equals("lop")) {
                assertEquals("created", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(0.4d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(0.4f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(11, e.getId());
            } else if (e.getVertex(Direction.OUT).getValue("name").equals("marko")) {
                assertEquals("knows", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(1.0d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(8, e.getId());
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v5 = (Vertex) g1.V().has("name", "ripple").next();
        assertEquals("java", v5.<String>getValue("lang"));
        assertEquals(2, v5.getPropertyKeys().size());
        if (g1.getFeatures().vertex().supportsUserSuppliedIds())
            assertEquals(5, v5.getId());

        final List<Edge> v5Edges = v5.bothE().toList();
        assertEquals(1, v5Edges.size());
        v5Edges.forEach(e -> {
            if (e.getVertex(Direction.OUT).getValue("name").equals("josh")) {
                assertEquals("created", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(1.0d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(1.0f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(10, e.getId());
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v6 = (Vertex) g1.V().has("name", "peter").next();
        assertEquals(35, v6.<Integer>getValue("age").intValue());
        assertEquals(2, v6.getPropertyKeys().size());
        if (g1.getFeatures().vertex().supportsUserSuppliedIds())
            assertEquals(6, v6.getId());

        final List<Edge> v6Edges = v6.bothE().toList();
        assertEquals(1, v6Edges.size());
        v6Edges.forEach(e -> {
            if (e.getVertex(Direction.IN).getValue("name").equals("lop")) {
                assertEquals("created", e.getLabel());
                if (lossyOfFloat)
                    assertEquals(0.2d, e.getValue("weight"), 0.0001d);
                else
                    assertEquals(0.2f, e.getValue("weight"), 0.0001f);
                assertEquals(1, e.getPropertyKeys().size());
                if (g1.getFeatures().edge().supportsUserSuppliedIds())
                    assertEquals(12, e.getId());
            } else {
                fail("Edge not expected");
            }
        });
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
        final GraphReader reader = new GraphMLReader.Builder().build();
        try (final InputStream stream = IoTest.class.getResourceAsStream(GRAPHML_RESOURCE_PATH_PREFIX + "graph-example-1.xml")) {
            reader.readGraph(stream, g);
        }
    }

    private static void readGraphSONIntoGraph(final Graph g) throws IOException {
        final GraphReader reader = new GraphSONReader.Builder().build();
        try (final InputStream stream = IoTest.class.getResourceAsStream(GRAPHSON_RESOURCE_PATH_PREFIX + "graph-example-1.json")) {
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
                jsonGenerator.writeStartObject();
                jsonGenerator.writeObjectField("cluster", customId.getCluster());
                jsonGenerator.writeObjectField("elementId", customId.getElementId().toString());
                jsonGenerator.writeEndObject();
            }
        }
    }
}
