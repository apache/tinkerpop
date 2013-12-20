package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.blueprints.Graph.Features.GraphPropertyFeatures;
import com.tinkerpop.blueprints.Graph.Features.PropertyFeatures;
import com.tinkerpop.blueprints.Graph.Features.VertexPropertyFeatures;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class PropertyTest extends AbstractBlueprintsTest {
    private static final String INVALID_FEATURE_SPECIFICATION = "Features for %s specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

    private static final Map testMap = new HashMap() {{
        put("testString", "try");
        put("testInteger", 123);
    }};

    private static final ArrayList mixedList = new ArrayList() {{
        add("try1");
        add(2);
    }};

    private static final ArrayList uniformStringList = new ArrayList() {{
        add("try1");
        add("try2");
    }};

    private static final ArrayList uniformIntegerList = new ArrayList() {{
        add(100);
        add(200);
        add(300);
    }};

    @Parameterized.Parameters(name = "{index}: supports{0}({1})")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {PropertyFeatures.FEATURE_BOOLEAN_VALUES, true},
                {PropertyFeatures.FEATURE_BOOLEAN_VALUES, false},
                {PropertyFeatures.FEATURE_DOUBLE_VALUES, Double.MIN_VALUE},
                {PropertyFeatures.FEATURE_DOUBLE_VALUES, Double.MAX_VALUE},
                {PropertyFeatures.FEATURE_DOUBLE_VALUES, 0.0d},
                {PropertyFeatures.FEATURE_DOUBLE_VALUES, 0.5d},
                {PropertyFeatures.FEATURE_DOUBLE_VALUES, -0.5d},
                {PropertyFeatures.FEATURE_FLOAT_VALUES, Float.MIN_VALUE},
                {PropertyFeatures.FEATURE_FLOAT_VALUES, Float.MAX_VALUE},
                {PropertyFeatures.FEATURE_FLOAT_VALUES, 0.0f},
                {PropertyFeatures.FEATURE_FLOAT_VALUES, 0.5f},
                {PropertyFeatures.FEATURE_FLOAT_VALUES, -0.5f},
                {PropertyFeatures.FEATURE_INTEGER_VALUES, Integer.MIN_VALUE},
                {PropertyFeatures.FEATURE_INTEGER_VALUES, Integer.MAX_VALUE},
                {PropertyFeatures.FEATURE_INTEGER_VALUES, 0},
                {PropertyFeatures.FEATURE_INTEGER_VALUES, 10000},
                {PropertyFeatures.FEATURE_INTEGER_VALUES, -10000},
                {PropertyFeatures.FEATURE_LONG_VALUES, Long.MIN_VALUE},
                {PropertyFeatures.FEATURE_LONG_VALUES, Long.MAX_VALUE},
                {PropertyFeatures.FEATURE_LONG_VALUES, 0l},
                {PropertyFeatures.FEATURE_LONG_VALUES, 10000l},
                {PropertyFeatures.FEATURE_LONG_VALUES, -10000l},
                {PropertyFeatures.FEATURE_MAP_VALUES, testMap},
                {PropertyFeatures.FEATURE_MIXED_LIST_VALUES, mixedList},
                {PropertyFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new boolean[]{true, false}},
                {PropertyFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new double[]{1d, 2d}},
                {PropertyFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new float[]{1f, 2f}},
                {PropertyFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new int[]{1, 2}},
                {PropertyFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new long[]{1l, 2l}},
                {PropertyFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new String[]{"try1", "try2"}},
                {PropertyFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new int[1]},
                {PropertyFeatures.FEATURE_SERIALIZABLE_VALUES, new MockSerializable("testing")},
                {PropertyFeatures.FEATURE_STRING_VALUES, "short string"},
                {PropertyFeatures.FEATURE_UNIFORM_LIST_VALUES, uniformIntegerList},
                {PropertyFeatures.FEATURE_UNIFORM_LIST_VALUES, uniformStringList}
        });
    }

    @Parameterized.Parameter(value = 0)
    public String featureName;

    @Parameterized.Parameter(value = 1)
    public Object value;

    @Ignore
    public void shouldSetValueOnEdge() throws Exception {
        assumeThat(g.getFeatures().supports(EdgePropertyFeatures.class, featureName), is(true));
        final Edge edge = createEdgeForPropertyFeatureTests();
        edge.setProperty("key", value);

        if (value instanceof Map)
            tryCommit(g, graph -> {
                final Map map = edge.<Map>getProperty("key").getValue();
                assertEquals(((Map) value).size(), map.size());
                ((Map) value).keySet().forEach(k -> assertEquals(((Map) value).get(k), map.get(k)));
            });
        else if (value instanceof List)
            tryCommit(g, graph -> {
                final List l = edge.<List>getProperty("key").getValue();
                assertEquals(((List) value).size(), l.size());
                for (int ix = 0; ix < ((List) value).size(); ix++) {
                    assertEquals(((List) value).get(ix), l.get(ix));
                }
            });
        else if (value instanceof MockSerializable)
            tryCommit(g, graph -> {
                final MockSerializable mock = edge.<MockSerializable>getProperty("key").getValue();
                assertEquals(((MockSerializable) value).getTestField(), mock.getTestField());
            });
        else
            tryCommit(g, graph -> assertEquals(value, edge.getProperty("key").getValue()));
    }

    /*@Test
    public void shouldSetValueOnGraph() throws Exception {
        assumeThat(g.getFeatures().supports(GraphPropertyFeatures.class, featureName), is(true));
        g.setProperty("key", value);

        if (value instanceof Map)
            tryCommit(g, graph -> {
                final Map map = graph.<Map>getProperty("key").getValue();
                assertEquals(((Map) value).size(), map.size());
                ((Map) value).keySet().forEach(k -> assertEquals(((Map) value).get(k), map.get(k)));
            });
        else if (value instanceof List)
            tryCommit(g, graph -> {
                final List l = graph.<List>getProperty("key").getValue();
                assertEquals(((List) value).size(), l.size());
                for (int ix = 0; ix < ((List) value).size(); ix++) {
                    assertEquals(((List) value).get(ix), l.get(ix));
                }
            });
        else if (value instanceof MockSerializable)
            tryCommit(g, graph -> {
                final MockSerializable mock = g.<MockSerializable>getProperty("key").getValue();
                assertEquals(((MockSerializable) value).getTestField(), mock.getTestField());
            });
        else
            tryCommit(g, graph->assertEquals(value, g.getProperty("key").getValue()));
    }*/

    @Test
    public void shouldSetValueOnVertex() throws Exception {
        assumeThat(g.getFeatures().supports(VertexPropertyFeatures.class, featureName), is(true));
        final Vertex v = g.addVertex("key", value);

        if (value instanceof Map)
            tryCommit(g, graph -> {
                final Map map = v.<Map>getProperty("key").getValue();
                assertEquals(((Map) value).size(), map.size());
                ((Map) value).keySet().forEach(k -> assertEquals(((Map) value).get(k), map.get(k)));
            });
        else if (value instanceof List)
            tryCommit(g, graph -> {
                final List l = v.<List>getProperty("key").getValue();
                assertEquals(((List) value).size(), l.size());
                for (int ix = 0; ix < ((List) value).size(); ix++) {
                    assertEquals(((List) value).get(ix), l.get(ix));
                }
            });
        else if (value instanceof MockSerializable)
            tryCommit(g, graph -> {
                final MockSerializable mock = v.<MockSerializable>getProperty("key").getValue();
                assertEquals(((MockSerializable) value).getTestField(), mock.getTestField());
            });
        else
            tryCommit(g, graph->assertEquals(value, v.getProperty("key").getValue()));
    }

    /*@Test
    public void shouldEnableFeatureOnEdgeIfNotEnabled() throws Exception {
        assumeThat(g.getFeatures().supports(EdgePropertyFeatures.class, featureName), is(false));
        try {
            final Edge edge = createEdgeForPropertyFeatureTests();
            edge.setProperty("key", value);
            fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphPropertyFeatures.class.getSimpleName(), featureName));
        } catch (UnsupportedOperationException e) {
            assertEquals(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value).getMessage(), e.getMessage());
        }
    }*/

    /*@Test
    public void shouldEnableFeatureOnGraphIfNotEnabled() throws Exception {
        assumeThat(g.getFeatures().supports(GraphPropertyFeatures.class, featureName), is(false));
        try {
            g.setProperty("key", value);
            fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphPropertyFeatures.class.getSimpleName(), featureName));
        } catch (UnsupportedOperationException e) {
            assertEquals(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value).getMessage(), e.getMessage());
        }
    }*/

    @Test
    public void shouldEnableFeatureOnVertexIfNotEnabled() throws Exception {
        assumeThat(g.getFeatures().supports(VertexPropertyFeatures.class, featureName), is(false));
        try {
            g.addVertex("key", value);
            fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphPropertyFeatures.class.getSimpleName(), featureName));
        } catch (UnsupportedOperationException e) {
            assertEquals(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value).getMessage(), e.getMessage());
        }
    }

    private Edge createEdgeForPropertyFeatureTests() {
        final Vertex vertexA = g.addVertex();
        final Vertex vertexB = g.addVertex();
        return vertexA.addEdge(BlueprintsStandardSuite.GraphManager.get().convertLabel("knows"), vertexB);
    }

    public static class MockSerializable implements Serializable {
        private String testField;

        public MockSerializable(final String testField) {
            this.testField = testField;
        }

        public String getTestField() {
            return this.testField;
        }

        public void setTestField(final String testField) {
            this.testField = testField;
        }

        @Override
        public boolean equals(Object oth) {
            if (this == oth) return true;
            else if (oth == null) return false;
            else if (!getClass().isInstance(oth)) return false;
            MockSerializable m = (MockSerializable) oth;
            if (testField == null) {
                return (m.testField == null);
            } else return testField.equals(m.testField);
        }
    }
}
