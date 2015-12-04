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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.LegacyGraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.util.CustomId;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.List;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_ANY_IDS;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures.FEATURE_VARIABLES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.*;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphson;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Enclosed.class)
public class IoTest {
    private static final Logger logger = LoggerFactory.getLogger(IoTest.class);

    public static class GraphMLTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
        public void shouldReadGraphML() throws IOException {
            readGraphMLIntoGraph(graph, "tinkerpop-classic.xml");
            assertClassicGraph(graph, false, true);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
        public void shouldReadGraphMLUnorderedElements() throws IOException {
            readGraphMLIntoGraph(graph, "tinkerpop-classic-unordered.xml");
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
        public void shouldReadGraphMLWithAllSupportedDataTypes() throws IOException {
            final GraphReader reader = GraphMLReader.build().create();
            try (final InputStream stream = IoTest.class.getResourceAsStream(TestHelper.convertPackageToResourcePath(GraphMLResourceAccess.class) + "graph-types.xml")) {
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
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_BOOLEAN_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_LONG_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_DOUBLE_VALUES)
        @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
        public void shouldReadGraphMLWithoutStrictOption() throws IOException {
            final GraphReader reader = GraphMLReader.build().strict(false).create();
            try (final InputStream stream = IoTest.class.getResourceAsStream(TestHelper.convertPackageToResourcePath(GraphMLResourceAccess.class) + "graph-types-bad.xml")) {
                reader.readGraph(stream, graph);
            }

            final Vertex v = graph.vertices().next();
            assertFalse(v.values("d").hasNext());
            assertEquals("some-string", v.<String>value("s"));
            assertFalse(v.values("i").hasNext());
            assertEquals(false, v.<Boolean>value("b"));
            assertFalse(v.values("f").hasNext());
            assertFalse(v.values("l").hasNext());
            assertEquals("junk", v.<String>value("n"));
        }

        @Test(expected = NumberFormatException.class)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_BOOLEAN_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_LONG_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_DOUBLE_VALUES)
        @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
        public void shouldReadGraphMLWithStrictOption() throws IOException {
            final GraphReader reader = GraphMLReader.build().strict(true).create();
            try (final InputStream stream = IoTest.class.getResourceAsStream(TestHelper.convertPackageToResourcePath(GraphMLResourceAccess.class) + "graph-types-bad.xml")) {
                reader.readGraph(stream, graph);
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

                final String expected = streamToString(IoTest.class.getResourceAsStream(TestHelper.convertPackageToResourcePath(GraphMLResourceAccess.class) + "tinkerpop-classic-normalized.xml"));
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
    }

    public static final class GraphSONTest extends AbstractGremlinTest {
        private Io.Builder<GraphSONIo> graphson;

        @Before
        public void setupBeforeEachTest() {
            graphson = graphson();
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
                final GraphSONMapper mapper = graph.io(graphson).mapper().normalize(true).create();
                final GraphSONWriter w = graph.io(graphson).writer().mapper(mapper).create();
                w.writeGraph(bos, graph);

                final String expected = streamToString(IoTest.class.getResourceAsStream(TestHelper.convertPackageToResourcePath(GraphSONResourceAccess.class) + "tinkerpop-classic-normalized.json"));
                assertEquals(expected.replace("\n", "").replace("\r", ""), bos.toString().replace("\n", "").replace("\r", ""));
            }
        }

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldReadWriteModernWrappedInJsonObject() throws Exception {
            try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                final GraphWriter writer = graph.io(graphson()).writer().wrapAdjacencyList(true).create();
                writer.writeGraph(os, graph);

                final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.MODERN);
                graphProvider.clear(configuration);
                final Graph g1 = graphProvider.openTestGraph(configuration);
                final GraphReader reader = graph.io(graphson()).reader().unwrapAdjacencyList(true).create();
                try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                    reader.readGraph(bais, g1);
                }

                // modern uses double natively so always assert as such
                IoTest.assertModernGraph(g1, true, true);

                graphProvider.clear(g1, configuration);
            }
        }

        /**
         * This is just a serialization check for JSON.
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
            final GraphWriter writer = graph.io(graphson).writer().mapper(
                    graph.io(graphson).mapper().addCustomModule(module).embedTypes(true).create()).create();

            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                writer.writeGraph(baos, graph);

                final JsonNode jsonGraph = new ObjectMapper().readTree(baos.toByteArray());
                final JsonNode idValue = jsonGraph.get(GraphSONTokens.ID);
                assertTrue(idValue.has("cluster"));
                assertEquals("vertex", idValue.get("cluster").asText());
                assertTrue(idValue.has("elementId"));
                assertEquals("AF4B5965-B176-4552-B3C1-FBBE2F52C305".toLowerCase(), idValue.get("elementId").get(1).asText());

                // reusing the same config used for creation of "g".
                final Configuration configuration = graphProvider.newGraphConfiguration("g2", this.getClass(), name.getMethodName(), null);
                graphProvider.clear(configuration);
                final Graph g2 = graphProvider.openTestGraph(configuration);

                try (final InputStream is = new ByteArrayInputStream(baos.toByteArray())) {
                    final GraphReader reader = graph.io(graphson).reader()
                            .mapper(graph.io(graphson).mapper().embedTypes(true).addCustomModule(module).create()).create();
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
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
        public void shouldReadLegacyGraphSON() throws IOException {
            final GraphReader reader = LegacyGraphSONReader.build().create();
            try (final InputStream stream = IoTest.class.getResourceAsStream(TestHelper.convertPackageToResourcePath(GraphSONResourceAccess.class) + "tinkerpop-classic-legacy.json")) {
                reader.readGraph(stream, graph);
            }

            // the id is lossy in migration because TP2 treated ID as String
            assertClassicGraph(graph, false, true);
        }
    }

    public static void assertCrewGraph(final Graph g1, final boolean lossyForId) {
        assertEquals(new Long(6), g1.traversal().V().count().next());
        assertEquals(new Long(14), g1.traversal().E().count().next());

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

        final Vertex v1 = g1.traversal().V().has("name", "marko").next();
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
                    assertWeightLoosely(0.5d, e);
                else
                    assertWeightLoosely(0.5f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 7);
            } else if (e.inVertex().value("name").equals("josh")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertWeightLoosely(1.0, e);
                else
                    assertWeightLoosely(1.0f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 8);
            } else if (e.inVertex().value("name").equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.4d, e);
                else
                    assertWeightLoosely(0.4f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 9);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v2 = g1.traversal().V().has("name", "vadas").next();
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
                    assertWeightLoosely(0.5d, e);
                else
                    assertWeightLoosely(0.5f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 7);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v3 = g1.traversal().V().has("name", "lop").next();
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
                    assertWeightLoosely(0.2d, e);
                else
                    assertWeightLoosely(0.2f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 12);
            } else if (e.outVertex().value("name").equals("josh")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.4d, e);
                else
                    assertWeightLoosely(0.4f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (e.outVertex().value("name").equals("marko")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.4d, e);
                else
                    assertWeightLoosely(0.4f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 9);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v4 = g1.traversal().V().has("name", "josh").next();
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
                    assertWeightLoosely(1.0d, e);
                else
                    assertWeightLoosely(1.0f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 10);
            } else if (e.inVertex().value("name").equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.4d, e);
                else
                    assertWeightLoosely(0.4f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (e.outVertex().value("name").equals("marko")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertWeightLoosely(1.0d, e);
                else
                    assertWeightLoosely(1.0f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 8);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v5 = g1.traversal().V().has("name", "ripple").next();
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
                    assertWeightLoosely(1.0d, e);
                else
                    assertWeightLoosely(1.0f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 10);
            } else {
                fail("Edge not expected");
            }
        });

        final Vertex v6 = g1.traversal().V().has("name", "peter").next();
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
                    assertWeightLoosely(0.2d, e);
                else
                    assertWeightLoosely(0.2f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 12);
            } else {
                fail("Edge not expected");
            }
        });
    }

    private static void assertWeightLoosely(final double expected, final Edge e) {
        try {
            assertEquals(expected, e.value("weight"), 0.0001d);
        } catch (Exception ex) {
            // for graphs that have strong typing via schema it is possible that a value that came across as graphson
            // with lossiness will end up having a value expected to double to be coerced to float by the underlying
            // graph.
            logger.warn("Attempting to assert weight as float for {} - if your graph is strongly typed from schema this is likely expected", e);
            assertEquals(new Double(expected).floatValue(), e.value("weight"), 0.0001f);
        }
    }

    private static void assertWeightLoosely(final float expected, final Edge e) {
        try {
            assertEquals(expected, e.value("weight"), 0.0001f);
        } catch (Exception ex) {
            // for graphs that have strong typing via schema it is possible that a value that came across as graphson
            // with lossiness will end up having a value expected to float to be coerced to double by the underlying
            // graph.
            logger.warn("Attempting to assert weight as double for {} - if your graph is strongly typed from schema this is likely expected", e);
            assertEquals(new Float(expected).doubleValue(), e.value("weight"), 0.0001d);
        }
    }

    private static void assertId(final Graph g, final boolean lossyForId, final Element e, final Object expected) {
        // it is possible that a Graph (e.g. elastic-gremlin) can supportUserSuppliedIds but internally
        // represent them as a value other than Numeric (which is what's in all of the test/toy data).
        // as we feature check for userSuppliedIds when asserting the identifier, we also ensure that
        // the id can be properly asserted for that Element before attempting to do so.  By asserting
        // at this level in this way, graphs can enjoy greater test coverage in IO.
        if ((e instanceof Vertex && g.features().vertex().supportsUserSuppliedIds() && g.features().vertex().supportsNumericIds())
                || (e instanceof Edge && g.features().edge().supportsUserSuppliedIds() && g.features().edge().supportsNumericIds())
                || (e instanceof VertexProperty && g.features().vertex().properties().supportsUserSuppliedIds()) && g.features().vertex().properties().supportsNumericIds()) {
            if (lossyForId)
                assertEquals(expected.toString(), e.id().toString());
            else
                assertEquals(expected, e.id());
        }
    }

    private static void validateXmlAgainstGraphMLXsd(final File file) throws Exception {
        final Source xmlFile = new StreamSource(file);
        final SchemaFactory schemaFactory = SchemaFactory
                .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema = schemaFactory.newSchema(IoTest.class.getResource(TestHelper.convertPackageToResourcePath(GraphMLResourceAccess.class) + "graphml-1.1.xsd"));
        final Validator validator = schema.newValidator();
        validator.validate(xmlFile);
    }

    private static void readGraphMLIntoGraph(final Graph g, final String file) throws IOException {
        final GraphReader reader = GraphMLReader.build().create();
        try (final InputStream stream = IoTest.class.getResourceAsStream(TestHelper.convertPackageToResourcePath(GraphMLResourceAccess.class) + file)) {
            reader.readGraph(stream, g);
        }
    }

    private static String streamToString(final InputStream in) throws IOException {
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

}
