package com.tinkerpop.gremlin.structure.blueprints;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
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
import static org.junit.Assume.assumeThat;

/**
 * Blueprints Test Suite for {@link com.tinkerpop.gremlin.structure.Property} operations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class PropertyTest {

    /**
     * Basic tests for the {@link com.tinkerpop.gremlin.structure.Property} class.
     */
    public static class BasicPropertyTest extends AbstractBlueprintsTest {
        @Test
        public void shouldHaveStandardStringRepresentation() {
            final Vertex v = g.addVertex("name", "marko");
            final Property p = v.getProperty("name");
            assertEquals(StringFactory.propertyString(p), p.toString());
        }
    }

    /**
     * Tests for feature support on {@link com.tinkerpop.gremlin.structure.Property}.  The tests validate if {@link com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures}
     * should be turned on or off and if the enabled features are properly supported by the implementation.  Note that
     * these tests are run in a separate test class as they are "parameterized" tests.
     */
    @RunWith(Parameterized.class)
    public static class PropertyFeatureSupportTest extends AbstractBlueprintsTest {
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

        @Test
        public void shouldSetValueOnEdge() throws Exception {
            assumeThat(g.getFeatures().supports(EdgePropertyFeatures.class, featureName), is(true));
            final Edge edge = createEdgeForPropertyFeatureTests();
            edge.setProperty("key", value);

            if (value instanceof Map)
                tryCommit(g, graph -> {
                    final Map map = edge.<Map>getProperty("key").get();
                    assertEquals(((Map) value).size(), map.size());
                    ((Map) value).keySet().forEach(k -> assertEquals(((Map) value).get(k), map.get(k)));
                });
            else if (value instanceof List)
                tryCommit(g, graph -> {
                    final List l = edge.<List>getProperty("key").get();
                    assertEquals(((List) value).size(), l.size());
                    for (int ix = 0; ix < ((List) value).size(); ix++) {
                        assertEquals(((List) value).get(ix), l.get(ix));
                    }
                });
            else if (value instanceof MockSerializable)
                tryCommit(g, graph -> {
                    final MockSerializable mock = edge.<MockSerializable>getProperty("key").get();
                    assertEquals(((MockSerializable) value).getTestField(), mock.getTestField());
                });
            else
                tryCommit(g, graph -> assertEquals(value, edge.getProperty("key").get()));
        }

        @Test
        public void shouldSetValueOnVertex() throws Exception {
            assumeThat(g.getFeatures().supports(VertexPropertyFeatures.class, featureName), is(true));
            final Vertex v = g.addVertex("key", value);

            if (value instanceof Map)
                tryCommit(g, graph -> {
                    final Map map = v.<Map>getProperty("key").get();
                    assertEquals(((Map) value).size(), map.size());
                    ((Map) value).keySet().forEach(k -> assertEquals(((Map) value).get(k), map.get(k)));
                });
            else if (value instanceof List)
                tryCommit(g, graph -> {
                    final List l = v.<List>getProperty("key").get();
                    assertEquals(((List) value).size(), l.size());
                    for (int ix = 0; ix < ((List) value).size(); ix++) {
                        assertEquals(((List) value).get(ix), l.get(ix));
                    }
                });
            else if (value instanceof MockSerializable)
                tryCommit(g, graph -> {
                    final MockSerializable mock = v.<MockSerializable>getProperty("key").get();
                    assertEquals(((MockSerializable) value).getTestField(), mock.getTestField());
                });
            else
                tryCommit(g, graph->assertEquals(value, v.getProperty("key").get()));
        }

        private Edge createEdgeForPropertyFeatureTests() {
            final Vertex vertexA = g.addVertex();
            final Vertex vertexB = g.addVertex();
            return vertexA.addEdge(BlueprintsStandardSuite.GraphManager.get().convertLabel("knows"), vertexB);
        }
    }

    private static class MockSerializable implements Serializable {
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
