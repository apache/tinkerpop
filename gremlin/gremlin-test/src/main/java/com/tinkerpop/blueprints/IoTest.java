package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.blueprints.Graph.Features.VertexPropertyFeatures;
import com.tinkerpop.blueprints.io.GraphReader;
import com.tinkerpop.blueprints.io.graphml.GraphMLReader;
import com.tinkerpop.blueprints.io.graphml.GraphMLWriter;
import com.tinkerpop.blueprints.util.StreamFactory;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.BufferedReader;
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

import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_FLOAT_VALUES;
import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_INTEGER_VALUES;
import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_STRING_VALUES;
import static org.junit.Assert.assertEquals;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoTest extends AbstractBlueprintsTest {

    // todo: should expand test here significantly.  see blueprints2

    private static final String RESOURCE_PATH_PREFIX = "/com/tinkerpop/blueprints/util/io/graphml/";

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    public void shouldReadGraphML() throws IOException {
        readGraphMLIntoGraph(g);

        assertEquals(6, StreamFactory.stream(g.query().vertices()).count());
        assertEquals(6, StreamFactory.stream(g.query().edges()).count());
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    public void shouldWriteNormalizedGraphML() throws Exception {
        readGraphMLIntoGraph(g);

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final GraphMLWriter w = new GraphMLWriter.Builder(g).setNormalize(true).build();
        w.outputGraph(bos);

        final String expected = streamToString(IoTest.class.getResourceAsStream(RESOURCE_PATH_PREFIX + "graph-example-1-normalized.xml"));
        assertEquals(expected.replace("\n", "").replace("\r", ""), bos.toString().replace("\n", "").replace("\r", ""));
    }

    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    public void shouldWriteNormalizedGraphMLWithEdgeLabel() throws Exception {
        readGraphMLIntoGraph(g);

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final GraphMLWriter w = new GraphMLWriter.Builder(g)
                .setNormalize(true)
                .setEdgeLabelKey("label").build();
        w.outputGraph(bos);

        String expected = streamToString(IoTest.class.getResourceAsStream(RESOURCE_PATH_PREFIX + "graph-example-1-schema-valid.xml"));
        assertEquals(expected.replace("\n", "").replace("\r", ""), bos.toString().replace("\n", "").replace("\r", ""));
    }

    /**
     * Note: this is only a very lightweight test of writer/reader encoding. It is known that there are characters
     * which, when written by GraphMLWriter, cause parse errors for GraphMLReader. However, this happens uncommonly
     * enough that is not yet known which characters those are.
     */
    @Test
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = EdgePropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
    public void shouldProperlyEncodeWithGraphML() throws Exception {
        final Vertex v = g.addVertex(Element.ID, "1");
        v.setProperty("text", "\u00E9");

        final GraphMLWriter w = new GraphMLWriter.Builder(g).build();

        final File f = File.createTempFile("test", "txt");
        try (final OutputStream out = new FileOutputStream(f)) {
            w.outputGraph(out);
        }

        validateXmlAgainstGraphMLXsd(f);

        // reusing the same config used for creation of "g".
        final Configuration configuration = graphProvider.newGraphConfiguration("g2");
        final Graph g2 = graphProvider.openTestGraph(configuration);
        final GraphMLReader r = new GraphMLReader.Builder(g2).build();

        try (final InputStream in = new FileInputStream(f)) {
            r.inputGraph(in);
        }

        final Vertex v2 = g2.query().ids("1").vertices().iterator().next();
        assertEquals("\u00E9", v2.getProperty("text").get());

        // need to manually close the "g2" instance
        graphProvider.clear(g2, configuration);
    }

    private void validateXmlAgainstGraphMLXsd(final File file) throws Exception {
        final Source xmlFile = new StreamSource(file);
        final SchemaFactory schemaFactory = SchemaFactory
                .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema = schemaFactory.newSchema(IoTest.class.getResource(RESOURCE_PATH_PREFIX + "graphml-1.1.xsd"));
        final Validator validator = schema.newValidator();
        validator.validate(xmlFile);
    }

    private static void readGraphMLIntoGraph(final Graph g) throws IOException {
        final GraphReader reader = new GraphMLReader.Builder(g).build();
        try (final InputStream stream = IoTest.class.getResourceAsStream(RESOURCE_PATH_PREFIX + "graph-example-1.xml")) {
            reader.inputGraph(stream);
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
}
