package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Graph.Features.MemoryFeatures;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * Blueprints Test Suite for {@link com.tinkerpop.gremlin.structure.Graph.Memory} operations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class MemoryTest {

    /**
     * Basic tests to ensure that {@link com.tinkerpop.gremlin.structure.Graph.Memory} have
     * appropriate {@link String} representations.
     */
    public static class StringRepresentationTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = MemoryFeatures.class, feature = MemoryFeatures.FEATURE_STRING_VALUES)
        public void testAnnotations() {
            final Graph.Memory memory = g.memory();
            memory.set("xo", "test1");
            memory.set("yo", "test2");
            memory.set("zo", "test3");

            tryCommit(g, graph -> assertEquals(StringFactory.memoryString(memory), memory.toString()));
        }
    }

    /**
     * Ensure that the {@link com.tinkerpop.gremlin.structure.Graph.Memory#asMap()} method returns some basics.
     * Other tests will enforce that all types are properly covered in {@link com.tinkerpop.gremlin.structure.Graph.Memory}.
     */
    public static class MemoryAsMapTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = MemoryFeatures.class, feature = MemoryFeatures.FEATURE_STRING_VALUES)
        public void testNone() {
            final Graph.Memory memory = g.memory();
            final Map<String,Object> mapOfAnnotations = memory.asMap();
            assertNotNull(mapOfAnnotations);
            assertEquals(0, mapOfAnnotations.size());
            try {
                mapOfAnnotations.put("something", "can't do this");
                fail("Should not be able to mutate the Map returned from Graph.memory.getAnnotations()");
            } catch (UnsupportedOperationException uoe) {

            }
        }

        @Test
        @FeatureRequirement(featureClass = MemoryFeatures.class, feature = MemoryFeatures.FEATURE_STRING_VALUES)
        public void testAnnotationString() {
            final Graph.Memory memory = g.memory();
            memory.set("test1", "1");
            memory.set("test2", "2");
            memory.set("test3", "3");

            tryCommit(g, graph -> {
                final Map<String, Object> m = memory.asMap();
                assertEquals("1", m.get("test1"));
                assertEquals("2", m.get("test2"));
                assertEquals("3", m.get("test3"));
            });
        }

        @Test
        @FeatureRequirement(featureClass = MemoryFeatures.class, feature = MemoryFeatures.FEATURE_INTEGER_VALUES)
        public void testAnnotationInteger() {
            final Graph.Memory memory = g.memory();
            memory.set("test1", 1);
            memory.set("test2", 2);
            memory.set("test3", 3);

            tryCommit(g, graph -> {
                final Map<String, Object> m = memory.asMap();
                assertEquals(1, m.get("test1"));
                assertEquals(2, m.get("test2"));
                assertEquals(3, m.get("test3"));
            });
        }

        @Test
        @FeatureRequirement(featureClass = MemoryFeatures.class, feature = MemoryFeatures.FEATURE_LONG_VALUES)
        public void testAnnotationLong() {
            final Graph.Memory memory = g.memory();
            memory.set("test1", 1l);
            memory.set("test2", 2l);
            memory.set("test3", 3l);

            tryCommit(g, graph -> {
                final Map<String, Object> m = memory.asMap();
                assertEquals(1l, m.get("test1"));
                assertEquals(2l, m.get("test2"));
                assertEquals(3l, m.get("test3"));
            });
        }

        @Test
        @FeatureRequirement(featureClass = MemoryFeatures.class, feature = MemoryFeatures.FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = MemoryFeatures.class, feature = MemoryFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = MemoryFeatures.class, feature = MemoryFeatures.FEATURE_LONG_VALUES)
        public void testAnnotationMixed() {
            final Graph.Memory memory = g.memory();
            memory.set("test1", "1");
            memory.set("test2", 2);
            memory.set("test3", 3l);

            tryCommit(g, graph -> {
                final Map<String, Object> m = memory.asMap();
                assertEquals("1", m.get("test1"));
                assertEquals(2, m.get("test2"));
                assertEquals(3l, m.get("test3"));
            });
        }
    }

    /**
     * Tests for feature support on {@link com.tinkerpop.gremlin.structure.Graph.Memory}.  The tests validate if
     * {@link com.tinkerpop.gremlin.structure.Graph.Features.AnnotationFeatures} should be turned on or off and if the
     * enabled features are properly supported by the implementation.  Note that these tests are run in a separate
     * test class as they are "parameterized" tests.
     */
    @RunWith(Parameterized.class)
    public static class MemoryFeatureSupportTest extends AbstractGremlinTest {
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
                    {MemoryFeatures.FEATURE_BOOLEAN_VALUES, true},
                    {MemoryFeatures.FEATURE_BOOLEAN_VALUES, false},
                    {MemoryFeatures.FEATURE_DOUBLE_VALUES, Double.MIN_VALUE},
                    {MemoryFeatures.FEATURE_DOUBLE_VALUES, Double.MAX_VALUE},
                    {MemoryFeatures.FEATURE_DOUBLE_VALUES, 0.0d},
                    {MemoryFeatures.FEATURE_DOUBLE_VALUES, 0.5d},
                    {MemoryFeatures.FEATURE_DOUBLE_VALUES, -0.5d},
                    {MemoryFeatures.FEATURE_FLOAT_VALUES, Float.MIN_VALUE},
                    {MemoryFeatures.FEATURE_FLOAT_VALUES, Float.MAX_VALUE},
                    {MemoryFeatures.FEATURE_FLOAT_VALUES, 0.0f},
                    {MemoryFeatures.FEATURE_FLOAT_VALUES, 0.5f},
                    {MemoryFeatures.FEATURE_FLOAT_VALUES, -0.5f},
                    {MemoryFeatures.FEATURE_INTEGER_VALUES, Integer.MIN_VALUE},
                    {MemoryFeatures.FEATURE_INTEGER_VALUES, Integer.MAX_VALUE},
                    {MemoryFeatures.FEATURE_INTEGER_VALUES, 0},
                    {MemoryFeatures.FEATURE_INTEGER_VALUES, 10000},
                    {MemoryFeatures.FEATURE_INTEGER_VALUES, -10000},
                    {MemoryFeatures.FEATURE_LONG_VALUES, Long.MIN_VALUE},
                    {MemoryFeatures.FEATURE_LONG_VALUES, Long.MAX_VALUE},
                    {MemoryFeatures.FEATURE_LONG_VALUES, 0l},
                    {MemoryFeatures.FEATURE_LONG_VALUES, 10000l},
                    {MemoryFeatures.FEATURE_LONG_VALUES, -10000l},
                    {MemoryFeatures.FEATURE_MAP_VALUES, testMap},
                    {MemoryFeatures.FEATURE_MIXED_LIST_VALUES, mixedList},
                    {MemoryFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new boolean[]{true, false}},
                    {MemoryFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new double[]{1d, 2d}},
                    {MemoryFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new float[]{1f, 2f}},
                    {MemoryFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new int[]{1, 2}},
                    {MemoryFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new long[]{1l, 2l}},
                    {MemoryFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new String[]{"try1", "try2"}},
                    {MemoryFeatures.FEATURE_PRIMITIVE_ARRAY_VALUES, new int[1]},
                    {MemoryFeatures.FEATURE_SERIALIZABLE_VALUES, new MockSerializable("testing")},
                    {MemoryFeatures.FEATURE_STRING_VALUES, "short string"},
                    {MemoryFeatures.FEATURE_UNIFORM_LIST_VALUES, uniformIntegerList},
                    {MemoryFeatures.FEATURE_UNIFORM_LIST_VALUES, uniformStringList}
            });
        }

        @Parameterized.Parameter(value = 0)
        public String featureName;

        @Parameterized.Parameter(value = 1)
        public Object value;

        @Test
        public void shouldSetValueOnGraph() throws Exception {
            assumeThat(g.getFeatures().supports(MemoryFeatures.class, featureName), is(true));
            final Graph.Memory memory = g.memory();
            memory.set("key", value);

            if (value instanceof Map)
                tryCommit(g, graph -> {
                    final Map map = memory.<Map>get("key");
                    assertEquals(((Map) value).size(), map.size());
                    ((Map) value).keySet().forEach(k -> assertEquals(((Map) value).get(k), map.get(k)));
                });
            else if (value instanceof List)
                tryCommit(g, graph -> {
                    final List l = memory.<List>get("key");
                    assertEquals(((List) value).size(), l.size());
                    for (int ix = 0; ix < ((List) value).size(); ix++) {
                        assertEquals(((List) value).get(ix), l.get(ix));
                    }
                });
            else if (value instanceof MockSerializable)
                tryCommit(g, graph -> {
                    final MockSerializable mock = memory.<MockSerializable>get("key");
                    assertEquals(((MockSerializable) value).getTestField(), mock.getTestField());
                });
            else
                tryCommit(g, graph -> assertEquals(value, memory.get("key")));
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
