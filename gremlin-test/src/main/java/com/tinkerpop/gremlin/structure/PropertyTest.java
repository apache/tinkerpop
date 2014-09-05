package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.ExceptionCoverage;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
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

import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * Gremlin Test Suite for {@link com.tinkerpop.gremlin.structure.Property} operations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class PropertyTest {

    /**
     * Basic tests for the {@link com.tinkerpop.gremlin.structure.Property} class.
     */
    public static class BasicPropertyTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldHaveStandardStringRepresentation() {
            final Vertex v = g.addVertex("name", "marko");
            final Property p = v.property("name");
            assertEquals(StringFactory.propertyString(p), p.toString());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldReturnEmptyPropertyIfKeyNonExistent() {
            final Vertex v = g.addVertex("name", "marko");
            tryCommit(g, (graph) -> {
                final Vertex v1 = g.v(v.id());
                final MetaProperty p = v1.property("nonexistent-key");
                assertEquals(MetaProperty.empty(), p);
            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldAllowRemovalWhenAlreadyRemoved() {
            final Vertex v = g.addVertex("name", "marko");
            tryCommit(g);
            final Vertex v1 = g.v(v.id());
            try {
                final Property p = v1.property("name");
                p.remove();
                p.remove();
            } catch (Exception ex) {
                fail("Removing a property that was already removed should not throw an exception");
            }
        }
    }

    /**
     * Checks that properties added to an {@link com.tinkerpop.gremlin.structure.Element} are validated in a consistent way when they are added at
     * {@link com.tinkerpop.gremlin.structure.Vertex} or {@link com.tinkerpop.gremlin.structure.Edge} construction by throwing an appropriate exception.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "providedKeyValuesMustBeAMultipleOfTwo",
            "providedKeyValuesMustHaveALegalKeyOnEvenIndices"
    })
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyValueCanNotBeNull",
            "propertyKeyCanNotBeEmpty"
    })
    public static class PropertyValidationOnAddExceptionConsistencyTest extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{index}: expect - {1}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {new Object[]{"odd", "number", "arguments"}, Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    {new Object[]{"odd"}, Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    {new Object[]{"odd", "number", 123, "test"}, Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    {new Object[]{"odd", null}, Property.Exceptions.propertyValueCanNotBeNull()},
                    {new Object[]{null, "val"}, Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    {new Object[]{"", "val"}, Property.Exceptions.propertyKeyCanNotBeEmpty()}});
        }

        @Parameterized.Parameter(value = 0)
        public Object[] arguments;

        @Parameterized.Parameter(value = 1)
        public Exception expectedException;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void shouldThrowOnGraphAddVertex() throws Exception {
            try {
                this.g.addVertex(arguments);
                fail(String.format("Call to addVertex should have thrown an exception with these arguments [%s]", arguments));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void shouldThrowOnGraphAddEdge() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v, arguments);
                fail(String.format("Call to addVertex should have thrown an exception with these arguments [%s]", arguments));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }

    /**
     * Test exceptions around use of {@link com.tinkerpop.gremlin.structure.Element#value(String)}.
     */
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyDoesNotExist"
    })
    public static class ElementGetValueExceptionConsistencyTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void shouldGetValueThatIsNotPresentOnVertex() {
            final Vertex v = g.addVertex();
            try {
                v.value("does-not-exist");
                fail("Call to Element.value() with a key that is not present should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Property.Exceptions.propertyDoesNotExist("does-not-exist");
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void shouldGetValueThatIsNotPresentOnEdge() {
            final Vertex v = g.addVertex();
            final Edge e = v.addEdge("label", v);
            try {
                e.value("does-not-exist");
                fail("Call to Element.value() with a key that is not present should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Property.Exceptions.propertyDoesNotExist("does-not-exist");
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }
    }


    /**
     * Checks that properties added to an {@link com.tinkerpop.gremlin.structure.Element} are validated in a consistent way when they are set after
     * {@link com.tinkerpop.gremlin.structure.Vertex} or {@link com.tinkerpop.gremlin.structure.Edge} construction by throwing an appropriate exception.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyValueCanNotBeNull",
            "propertyKeyCanNotBeNull",
            "propertyKeyIdIsReserved",
            "propertyKeyLabelIsReserved",
            "propertyKeyCanNotBeEmpty"
    })
    public static class PropertyValidationOnSetExceptionConsistencyTest extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{index}: expect - {2}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"k", null, Property.Exceptions.propertyValueCanNotBeNull()},
                    {null, "v", Property.Exceptions.propertyKeyCanNotBeNull()},
                    {Element.ID, "v", Property.Exceptions.propertyKeyIdIsReserved()},
                    {Element.LABEL, "v", Property.Exceptions.propertyKeyLabelIsReserved()},
                    {"", "v", Property.Exceptions.propertyKeyCanNotBeEmpty()}});
        }

        @Parameterized.Parameter(value = 0)
        public String key;

        @Parameterized.Parameter(value = 1)
        public String val;

        @Parameterized.Parameter(value = 2)
        public Exception expectedException;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void testGraphVertexSetPropertyStandard() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.property(key, val);
                fail(String.format("Call to Vertex.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void shouldThrowOnGraphEdgeSetPropertyStandard() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v).property(key, val);
                fail(String.format("Call to Edge.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }

    /**
     * Tests for feature support on {@link com.tinkerpop.gremlin.structure.Property}.  The tests validate if {@link com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures}
     * should be turned on or off and if the enabled features are properly supported by the implementation.  Note that
     * these tests are run in a separate test class as they are "parameterized" tests.
     */
    @RunWith(Parameterized.class)
    public static class PropertyFeatureSupportTest extends AbstractGremlinTest {
        private static final Map<String,Object> testMap = new HashMap<String,Object>() {{
            put("testString", "try");
            put("testInteger", 123);
        }};

        private static final ArrayList<Object> mixedList = new ArrayList<Object>() {{
            add("try1");
            add(2);
        }};

        private static final ArrayList<String> uniformStringList = new ArrayList<String>() {{
            add("try1");
            add("try2");
        }};

        private static final ArrayList<Integer> uniformIntegerList = new ArrayList<Integer>() {{
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
                    {PropertyFeatures.FEATURE_STRING_ARRAY_VALUES, new boolean[]{true, false}},
                    {PropertyFeatures.FEATURE_DOUBLE_ARRAY_VALUES, new double[]{1d, 2d}},
                    {PropertyFeatures.FEATURE_FLOAT_ARRAY_VALUES, new float[]{1f, 2f}},
                    {PropertyFeatures.FEATURE_INTEGER_ARRAY_VALUES, new int[]{1, 2}},
                    {PropertyFeatures.FEATURE_LONG_ARRAY_VALUES, new long[]{1l, 2l}},
                    {PropertyFeatures.FEATURE_STRING_ARRAY_VALUES, new String[]{"try1", "try2"}},
                    {PropertyFeatures.FEATURE_INTEGER_ARRAY_VALUES, new int[1]},
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
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldSetValueOnEdge() throws Exception {
            assumeThat(g.features().supports(EdgePropertyFeatures.class, featureName), is(true));
            final Edge edge = createEdgeForPropertyFeatureTests();
            edge.property("key", value);
            assertPropertyValue(edge);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldSetValueOnVertex() throws Exception {
            assumeThat(g.features().supports(VertexPropertyFeatures.class, featureName), is(true));
            final Vertex vertex = g.addVertex("key", value);
            assertPropertyValue(vertex);
        }

        private void assertPropertyValue(final Element element) {
            if (value instanceof Map)
                tryCommit(g, graph -> {
                    final Map map = element.<Map>property("key").value();
                    assertEquals(((Map) value).size(), map.size());
                    ((Map) value).keySet().forEach(k -> assertEquals(((Map) value).get(k), map.get(k)));
                });
            else if (value instanceof List)
                tryCommit(g, graph -> {
                    final List l = element.<List>property("key").value();
                    assertEquals(((List) value).size(), l.size());
                    for (int ix = 0; ix < ((List) value).size(); ix++) {
                        assertEquals(((List) value).get(ix), l.get(ix));
                    }
                });
            else if (value instanceof MockSerializable)
                tryCommit(g, graph -> {
                    final MockSerializable mock = element.<MockSerializable>property("key").value();
                    assertEquals(((MockSerializable) value).getTestField(), mock.getTestField());
                });
            else if (value instanceof boolean[])
                tryCommit(g, graph -> {
                    final boolean[] l = element.<boolean[]>property("key").value();
                    assertEquals(((boolean[]) value).length, l.length);
                    for (int ix = 0; ix < ((boolean[]) value).length; ix++) {
                        assertEquals(((boolean[]) value)[ix], l[ix]);
                    }
                });
            else if (value instanceof double[])
                tryCommit(g, graph -> {
                    final double[] l = element.<double[]>property("key").value();
                    assertEquals(((double[]) value).length, l.length);
                    for (int ix = 0; ix < ((double[]) value).length; ix++) {
                        assertEquals(((double[]) value)[ix], l[ix], 0.0d);
                    }
                });
            else if (value instanceof float[])
                tryCommit(g, graph -> {
                    final float[] l = element.<float[]>property("key").value();
                    assertEquals(((float[]) value).length, l.length);
                    for (int ix = 0; ix < ((float[]) value).length; ix++) {
                        assertEquals(((float[]) value)[ix], l[ix], 0.0f);
                    }
                });
            else if (value instanceof int[])
                tryCommit(g, graph -> {
                    final int[] l = element.<int[]>property("key").value();
                    assertEquals(((int[]) value).length, l.length);
                    for (int ix = 0; ix < ((int[]) value).length; ix++) {
                        assertEquals(((int[]) value)[ix], l[ix]);
                    }
                });
            else if (value instanceof long[])
                tryCommit(g, graph -> {
                    final long[] l = element.<long[]>property("key").value();
                    assertEquals(((long[]) value).length, l.length);
                    for (int ix = 0; ix < ((long[]) value).length; ix++) {
                        assertEquals(((long[]) value)[ix], l[ix]);
                    }
                });
            else if (value instanceof String[])
                tryCommit(g, graph -> {
                    final String[] l = element.<String[]>property("key").value();
                    assertEquals(((String[]) value).length, l.length);
                    for (int ix = 0; ix < ((String[]) value).length; ix++) {
                        assertEquals(((String[]) value)[ix], l[ix]);
                    }
                });
            else
                tryCommit(g, graph -> assertEquals(value, element.property("key").value()));
        }

        private Edge createEdgeForPropertyFeatureTests() {
            final Vertex vertexA = g.addVertex();
            final Vertex vertexB = g.addVertex();
            return vertexA.addEdge(GraphManager.get().convertLabel("knows"), vertexB);
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
