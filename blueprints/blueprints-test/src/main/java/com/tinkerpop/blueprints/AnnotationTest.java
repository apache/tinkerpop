package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.Graph.Features.AnnotationFeatures;
import com.tinkerpop.blueprints.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.blueprints.Graph.Features.GraphAnnotationFeatures;
import com.tinkerpop.blueprints.Graph.Features.VertexAnnotationFeatures;
import com.tinkerpop.blueprints.Graph.Features.VertexPropertyFeatures;
import com.tinkerpop.blueprints.util.StringFactory;
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
 * Blueprints Test Suite for {@link com.tinkerpop.blueprints.Annotations} and {@link AnnotatedList} operations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class AnnotationTest {

    /**
     * Basic tests to ensure that {@link Annotations}, {@link AnnotatedList}, and {@link AnnotatedValue} have
     * appropriate {@link String} representations.
     */
    public static class StringRepresentationTest extends AbstractBlueprintsTest {
        @Test
        @FeatureRequirement(featureClass = VertexAnnotationFeatures.class, feature = VertexAnnotationFeatures.FEATURE_STRING_VALUES)
        public void testAnnotatedList() {
            final Vertex v = g.addVertex();
            v.setProperty("names", AnnotatedList.make());
            final Property<AnnotatedList<String>> names = v.getProperty("names");
            names.get().addValue("antonio", "time", 1);
            names.get().addValue("antonio", "time", 2);
            names.get().addValue("antonio", "time", 3);

            tryCommit(g, graph->assertEquals(StringFactory.annotatedListString(names.get()), names.get().toString()));
            tryCommit(g, graph->assertEquals(StringFactory.propertyString(names), names.toString()));
        }

        @Test
        @FeatureRequirement(featureClass = VertexAnnotationFeatures.class, feature = VertexAnnotationFeatures.FEATURE_STRING_VALUES)
        public void testAnnotatedValue() {
            final Vertex v = g.addVertex();
            v.setProperty("names", AnnotatedList.make());
            final Property<AnnotatedList<String>> names = v.getProperty("names");
            final AnnotatedValue av = names.get().addValue("antonio", "time", 1);

            tryCommit(g, graph->assertEquals(StringFactory.annotatedValueString(av), av.toString()));
        }

        @Test
        @FeatureRequirement(featureClass = GraphAnnotationFeatures.class, feature = GraphAnnotationFeatures.FEATURE_STRING_VALUES)
        public void testAnnotations() {
            final Annotations annotations = g.annotations();
            annotations.set("xo", "test1");
            annotations.set("yo", "test2");
            annotations.set("zo", "test3");

            tryCommit(g, graph -> assertEquals(StringFactory.annotationsString(annotations), annotations.toString()));
        }
    }

    /**
     * Tests for feature support on {@link Annotations} and {@link AnnotatedList}.  The tests validate if
     * {@link com.tinkerpop.blueprints.Graph.Features.AnnotationFeatures} should be turned on or off and if the
     * enabled features are properly supported by the implementation.  Note that these tests are run in a separate
     * test class as they are "parameterized" tests.
     */
    @RunWith(Parameterized.class)
    public static class AnnotationFeatureSupportTest extends AbstractBlueprintsTest {
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
                    {AnnotationFeatures.FEATURE_BOOLEAN_VALUES, true},
                    {AnnotationFeatures.FEATURE_BOOLEAN_VALUES, false},
                    {AnnotationFeatures.FEATURE_DOUBLE_VALUES, Double.MIN_VALUE},
                    {AnnotationFeatures.FEATURE_DOUBLE_VALUES, Double.MAX_VALUE},
                    {AnnotationFeatures.FEATURE_DOUBLE_VALUES, 0.0d},
                    {AnnotationFeatures.FEATURE_DOUBLE_VALUES, 0.5d},
                    {AnnotationFeatures.FEATURE_DOUBLE_VALUES, -0.5d},
                    {AnnotationFeatures.FEATURE_FLOAT_VALUES, Float.MIN_VALUE},
                    {AnnotationFeatures.FEATURE_FLOAT_VALUES, Float.MAX_VALUE},
                    {AnnotationFeatures.FEATURE_FLOAT_VALUES, 0.0f},
                    {AnnotationFeatures.FEATURE_FLOAT_VALUES, 0.5f},
                    {AnnotationFeatures.FEATURE_FLOAT_VALUES, -0.5f},
                    {AnnotationFeatures.FEATURE_INTEGER_VALUES, Integer.MIN_VALUE},
                    {AnnotationFeatures.FEATURE_INTEGER_VALUES, Integer.MAX_VALUE},
                    {AnnotationFeatures.FEATURE_INTEGER_VALUES, 0},
                    {AnnotationFeatures.FEATURE_INTEGER_VALUES, 10000},
                    {AnnotationFeatures.FEATURE_INTEGER_VALUES, -10000},
                    {AnnotationFeatures.FEATURE_LONG_VALUES, Long.MIN_VALUE},
                    {AnnotationFeatures.FEATURE_LONG_VALUES, Long.MAX_VALUE},
                    {AnnotationFeatures.FEATURE_LONG_VALUES, 0l},
                    {AnnotationFeatures.FEATURE_LONG_VALUES, 10000l},
                    {AnnotationFeatures.FEATURE_LONG_VALUES, -10000l},
                    {AnnotationFeatures.FEATURE_MAP_VALUES, testMap},
                    {AnnotationFeatures.FEATURE_MIXED_LIST_VALUES, mixedList},
                    {AnnotationFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new boolean[]{true, false}},
                    {AnnotationFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new double[]{1d, 2d}},
                    {AnnotationFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new float[]{1f, 2f}},
                    {AnnotationFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new int[]{1, 2}},
                    {AnnotationFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new long[]{1l, 2l}},
                    {AnnotationFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new String[]{"try1", "try2"}},
                    {AnnotationFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new int[1]},
                    {AnnotationFeatures.FEATURE_SERIALIZABLE_VALUES, new MockSerializable("testing")},
                    {AnnotationFeatures.FEATURE_STRING_VALUES, "short string"},
                    {AnnotationFeatures.FEATURE_UNIFORM_LIST_VALUES, uniformIntegerList},
                    {AnnotationFeatures.FEATURE_UNIFORM_LIST_VALUES, uniformStringList}
            });
        }

        @Parameterized.Parameter(value = 0)
        public String featureName;

        @Parameterized.Parameter(value = 1)
        public Object value;

        @Test
        public void shouldSetValueOnGraph() throws Exception {
            assumeThat(g.getFeatures().supports(EdgePropertyFeatures.class, featureName), is(true));
            final Annotations annotations = g.annotations();
            annotations.set("key", value);

            if (value instanceof Map)
                tryCommit(g, graph -> {
                    final Map map = annotations.<Map>get("key").get();
                    assertEquals(((Map) value).size(), map.size());
                    ((Map) value).keySet().forEach(k -> assertEquals(((Map) value).get(k), map.get(k)));
                });
            else if (value instanceof List)
                tryCommit(g, graph -> {
                    final List l = annotations.<List>get("key").get();
                    assertEquals(((List) value).size(), l.size());
                    for (int ix = 0; ix < ((List) value).size(); ix++) {
                        assertEquals(((List) value).get(ix), l.get(ix));
                    }
                });
            else if (value instanceof MockSerializable)
                tryCommit(g, graph -> {
                    final MockSerializable mock = annotations.<MockSerializable>get("key").get();
                    assertEquals(((MockSerializable) value).getTestField(), mock.getTestField());
                });
            else
                tryCommit(g, graph -> assertEquals(value, annotations.get("key").get()));
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
