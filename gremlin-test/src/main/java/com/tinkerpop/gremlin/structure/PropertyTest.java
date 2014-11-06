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
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;
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
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeThat;

/**
 * Gremlin Test Suite for {@link com.tinkerpop.gremlin.structure.Property} operations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
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
                final VertexProperty p = v1.property("nonexistent-key");
                assertEquals(VertexProperty.empty(), p);
            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldAllowRemovalFromVertexWhenAlreadyRemoved() {
            final Vertex v = g.addVertex("name", "marko");
            tryCommit(g);
            final Vertex v1 = g.v(v.id());
            try {
                final Property p = v1.property("name");
                p.remove();
                p.remove();
                v1.property("name").remove();
                v1.property("name").remove();
            } catch (Exception ex) {
                fail("Removing a vertex property that was already removed should not throw an exception");
            }
        }


        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldAllowRemovalFromEdgeWhenAlreadyRemoved() {
            final Vertex v = g.addVertex("name", "marko");
            tryCommit(g);
            final Vertex v1 = g.v(v.id());

            try {
                final Edge edge = v1.addEdge("knows", g.addVertex());
                final Property p = edge.property("stars", 5);
                p.remove();
                p.remove();
                edge.property("stars").remove();
                edge.property("stars").remove();
            } catch (Exception ex) {
                fail("Removing an edge property that was already removed should not throw an exception");
            }
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldReturnHiddenKeysWithOutHiddenPrefix() {
            final Vertex v = g.addVertex("name", "marko", Graph.Key.hide("acl"), "rw", Graph.Key.hide("other"), "rw", "acl", "r");
            tryCommit(g);
            final Vertex v1 = g.v(v.id());
            assertEquals(2, v1.hiddenKeys().size());
            assertTrue(v1.hiddenKeys().stream().allMatch(key -> !Graph.Key.isHidden(key)));
            assertTrue(v1.hiddenKeys().stream().allMatch(k -> k.equals("acl") || k.equals("other")));
            assertEquals("rw", v1.iterators().hiddenPropertyIterator("acl").next().value());
            assertEquals("r", v1.iterators().propertyIterator("acl").next().value());
            assertEquals("acl", v1.property(Graph.Key.hide("acl")).key());
            assertEquals("other", v1.property(Graph.Key.hide("other")).key());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldNotReHideAnAlreadyHiddenKeyWhenGettingHiddenValue() {
            final Vertex v = g.addVertex("name", "marko", Graph.Key.hide("acl"), "rw", Graph.Key.hide("other"), "rw");
            tryCommit(g);
            final Vertex v1 = g.v(v.id());
            v1.hiddenKeys().stream().forEach(hiddenKey -> assertTrue(v1.hiddenValues(hiddenKey).hasNext()));
            assertFalse(v1.hiddenValues(Graph.Key.hide("other")).hasNext());
            assertTrue(v1.hiddenValues("other").hasNext());

            final Vertex u = g.addVertex();
            Edge e = v1.addEdge("knows", u, Graph.Key.hide("acl"), "private", "acl", "public");
            tryCommit(g);
            final Edge e1 = g.e(e.id());
            e1.hiddenKeys().stream().forEach(hiddenKey -> assertTrue(e1.hiddenValues(hiddenKey).hasNext()));
            assertFalse(e1.hiddenValues(Graph.Key.hide("acl")).hasNext());
            assertTrue(e1.hiddenValues("acl").hasNext());
            assertEquals("private", e1.iterators().hiddenPropertyIterator("acl").next().value());
            assertEquals("public", e1.iterators().propertyIterator("acl").next().value());
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
                validateException(expectedException, ex);
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
                validateException(expectedException, ex);
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
                validateException(Property.Exceptions.propertyDoesNotExist("does-not-exist"), ex);
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
                validateException(Property.Exceptions.propertyDoesNotExist("does-not-exist"), ex);
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
            "propertyKeyCanNotBeEmpty",
            "propertyKeyCanNotBeASystemKey"
    })
    public static class PropertyValidationOnSetExceptionConsistencyTest extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{index}: expect - {2}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"k", null, Property.Exceptions.propertyValueCanNotBeNull()},
                    {null, "v", Property.Exceptions.propertyKeyCanNotBeNull()},
                    {"", "v", Property.Exceptions.propertyKeyCanNotBeEmpty()},
                    {Graph.System.system("systemKey"), "value", Property.Exceptions.propertyKeyCanNotBeASystemKey(Graph.System.system("systemKey"))}});
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
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = VertexPropertyFeatures.FEATURE_ADD_PROPERTY)
        public void testGraphVertexSetPropertyStandard() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.property(key, val);
                fail(String.format("Call to Vertex.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                validateException(expectedException, ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
        public void shouldThrowOnGraphEdgeSetPropertyStandard() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v).property(key, val);
                fail(String.format("Call to Edge.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                validateException(expectedException, ex);
            }
        }
    }

    /**
     * This set of tests didn't fit well in VertexProperty as they do not relate to edges and they did not work
     * well in Edge tests either as they don't use an "Enclosed" runner.  This seemed like the next best place.
     */
    @RunWith(Parameterized.class)
    public static class EdgePropertiesShouldHideCorrectly extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{index}: {0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, Edge, Boolean>>> tests = new ArrayList<>();
            tests.add(Pair.with("e.property(\"age\").isPresent()", (Graph g, Edge e) -> e.property("age").isPresent()));
            tests.add(Pair.with("e.value(\"age\").equals(16)", (Graph g, Edge e) -> e.value("age").equals(16)));
            tests.add(Pair.with("e.properties(\"age\").count().next().intValue() == 1", (Graph g, Edge e) -> e.properties("age").count().next().intValue() == 1));
            tests.add(Pair.with("e.properties(\"age\").value().next().equals(16)", (Graph g, Edge e) -> e.properties("age").value().next().equals(16)));
            tests.add(Pair.with("e.hiddens(\"age\").count().next().intValue() == 1", (Graph g, Edge e) -> e.hiddens("age").count().next().intValue() == 1));
            tests.add(Pair.with("e.hiddens(Graph.Key.hide(\"age\")).count().next().intValue() == 0", (Graph g, Edge e) -> e.hiddens(Graph.Key.hide("age")).count().next().intValue() == 0));
            tests.add(Pair.with("e.properties(Graph.Key.hide(\"age\")).count().next() == 0", (Graph g, Edge e) -> e.properties(Graph.Key.hide("age")).count().next().intValue() == 0));
            tests.add(Pair.with("e.propertyMap(Graph.Key.hide(\"age\")).next().size() == 0", (Graph g, Edge e) -> e.propertyMap(Graph.Key.hide("age")).next().size() == 0));
            tests.add(Pair.with("e.valueMap(Graph.Key.hide(\"age\")).next().size() == 0", (Graph g, Edge e) -> e.valueMap(Graph.Key.hide("age")).next().size() == 0));
            tests.add(Pair.with("e.propertyMap(\"age\").next().size() == 1", (Graph g, Edge e) -> e.propertyMap("age").next().size() == 1));
            tests.add(Pair.with("e.valueMap(\"age\").next().size() == 1", (Graph g, Edge e) -> e.valueMap("age").next().size() == 1));
            tests.add(Pair.with("e.hiddenMap(Graph.Key.hide(\"age\")).next().size() == 0", (Graph g, Edge e) -> e.hiddenMap(Graph.Key.hide("age")).next().size() == 0));
            tests.add(Pair.with("e.hiddenMap(\"age\").next().size() == 1", (Graph g, Edge e) -> e.hiddenMap("age").next().size() == 1));
            tests.add(Pair.with("e.hiddenValueMap(Graph.Key.hide(\"age\")).next().size() == 0", (Graph g, Edge e) -> e.hiddenValueMap(Graph.Key.hide("age")).next().size() == 0));
            tests.add(Pair.with("e.hiddenValueMap(\"age\").next().size() == 1", (Graph g, Edge e) -> e.hiddenValueMap("age").next().size() == 1));
            tests.add(Pair.with("e.hiddens(\"age\").value().toList().contains(29)", (Graph g, Edge e) -> e.hiddens("age").value().toList().contains(29)));
            tests.add(Pair.with("e.hiddenKeys().size() == 2", (Graph g, Edge e) -> e.hiddenKeys().size() == 2));
            tests.add(Pair.with("e.keys().size() == 3", (Graph g, Edge e) -> e.keys().size() == 3));
            tests.add(Pair.with("e.keys().contains(\"age\")", (Graph g, Edge e) -> e.keys().contains("age")));
            tests.add(Pair.with("e.keys().contains(\"name\")", (Graph g, Edge e) -> e.keys().contains("name")));
            tests.add(Pair.with("e.hiddenKeys().contains(\"age\")", (Graph g, Edge e) -> e.hiddenKeys().contains("age")));
            tests.add(Pair.with("e.property(Graph.Key.hide(\"color\")).key().equals(\"color\")", (Graph g, Edge e) -> e.property(Graph.Key.hide("color")).key().equals("color")));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().propertyIterator(Graph.Key.hide(\"color\"))).count() == 0", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().propertyIterator(Graph.Key.hide("color"))).count() == 0));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().propertyIterator(Graph.Key.hide(\"age\"))).count() == 0", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().propertyIterator(Graph.Key.hide("age"))).count() == 0));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().propertyIterator(\"age\")).count() == 1", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().propertyIterator("age")).count() == 1));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().hiddenPropertyIterator(Graph.Key.hide(\"color\"))).count() == 0", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().hiddenPropertyIterator(Graph.Key.hide("color"))).count() == 0));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().hiddenPropertyIterator(Graph.Key.hide(\"age\"))).count() == 0", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().hiddenPropertyIterator(Graph.Key.hide("age"))).count() == 0));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().hiddenPropertyIterator(\"color\")).count() == 1", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().hiddenPropertyIterator("color")).count() == 1));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().hiddenPropertyIterator(\"age\")).count() == 1", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().hiddenPropertyIterator("age")).count() == 1));

            return tests.stream().map(d -> {
                final Object[] o = new Object[2];
                o[0] = d.getValue0();
                o[1] = d.getValue1();
                return o;
            }).collect(Collectors.toList());
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public BiFunction<Graph, Edge, Boolean> streamGetter;

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        public void shouldHandleHiddenVertexProperties() {
            final Vertex v = g.addVertex();
            final Edge e = v.addEdge("self", v, Graph.Key.hide("age"), 29, "age", 16, "name", "marko", "food", "taco", Graph.Key.hide("color"), "purple");
            tryCommit(g, g -> {
                assertTrue(streamGetter.apply(g, e));
            });
        }
    }

    /**
     * Tests for feature support on {@link com.tinkerpop.gremlin.structure.Property}.  The tests validate if {@link com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures}
     * should be turned on or off and if the enabled features are properly supported by the implementation.  Note that
     * these tests are run in a separate test class as they are "parameterized" tests.
     */
    @RunWith(Parameterized.class)
    public static class PropertyFeatureSupportTest extends AbstractGremlinTest {
        private static final Map<String,Object> testMap = new HashMap<>();

        private static final ArrayList<Object> mixedList = new ArrayList<>();

        private static final ArrayList<String> uniformStringList = new ArrayList<>();

        private static final ArrayList<Integer> uniformIntegerList = new ArrayList<>();

        static {
            testMap.put("testString", "try");
            testMap.put("testInteger", 123);

            mixedList.add("try1");
            mixedList.add(2);

            uniformStringList.add("try1");
            uniformStringList.add("try2");

            uniformIntegerList.add(100);
            uniformIntegerList.add(200);
            uniformIntegerList.add(300);
        }

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
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
        public void shouldSetValueOnEdge() throws Exception {
            assumeThat(g.features().supports(EdgePropertyFeatures.class, featureName), is(true));
            final Edge edge = createEdgeForPropertyFeatureTests();
            edge.property("aKey", value);
            assertPropertyValue(edge);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
        public void shouldSetValueOnVertex() throws Exception {
            assumeThat(g.features().supports(VertexPropertyFeatures.class, featureName), is(true));
            final Vertex vertex = g.addVertex();
            vertex.property("aKey", value);
            assertPropertyValue(vertex);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldSetValueOnEdgeOnAdd() throws Exception {
            assumeThat(g.features().supports(EdgePropertyFeatures.class, featureName), is(true));
            final Edge edge = createEdgeForPropertyFeatureTests("aKey", value);
            assertPropertyValue(edge);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldSetValueOnVertexOnAdd() throws Exception {
            assumeThat(g.features().supports(VertexPropertyFeatures.class, featureName), is(true));
            final Vertex vertex = g.addVertex("aKey", value);
            assertPropertyValue(vertex);
        }

        private void assertPropertyValue(final Element element) {
            if (value instanceof Map)
                tryCommit(g, graph -> {
                    final Map map = element.<Map>property("aKey").value();
                    assertEquals(((Map) value).size(), map.size());
                    ((Map) value).keySet().forEach(k -> assertEquals(((Map) value).get(k), map.get(k)));
                });
            else if (value instanceof List)
                tryCommit(g, graph -> {
                    final List l = element.<List>property("aKey").value();
                    assertEquals(((List) value).size(), l.size());
                    for (int ix = 0; ix < ((List) value).size(); ix++) {
                        assertEquals(((List) value).get(ix), l.get(ix));
                    }
                });
            else if (value instanceof MockSerializable)
                tryCommit(g, graph -> {
                    final MockSerializable mock = element.<MockSerializable>property("aKey").value();
                    assertEquals(((MockSerializable) value).getTestField(), mock.getTestField());
                });
            else if (value instanceof boolean[])
                tryCommit(g, graph -> {
                    final boolean[] l = element.<boolean[]>property("aKey").value();
                    assertEquals(((boolean[]) value).length, l.length);
                    for (int ix = 0; ix < ((boolean[]) value).length; ix++) {
                        assertEquals(((boolean[]) value)[ix], l[ix]);
                    }
                });
            else if (value instanceof double[])
                tryCommit(g, graph -> {
                    final double[] l = element.<double[]>property("aKey").value();
                    assertEquals(((double[]) value).length, l.length);
                    for (int ix = 0; ix < ((double[]) value).length; ix++) {
                        assertEquals(((double[]) value)[ix], l[ix], 0.0d);
                    }
                });
            else if (value instanceof float[])
                tryCommit(g, graph -> {
                    final float[] l = element.<float[]>property("aKey").value();
                    assertEquals(((float[]) value).length, l.length);
                    for (int ix = 0; ix < ((float[]) value).length; ix++) {
                        assertEquals(((float[]) value)[ix], l[ix], 0.0f);
                    }
                });
            else if (value instanceof int[])
                tryCommit(g, graph -> {
                    final int[] l = element.<int[]>property("aKey").value();
                    assertEquals(((int[]) value).length, l.length);
                    for (int ix = 0; ix < ((int[]) value).length; ix++) {
                        assertEquals(((int[]) value)[ix], l[ix]);
                    }
                });
            else if (value instanceof long[])
                tryCommit(g, graph -> {
                    final long[] l = element.<long[]>property("aKey").value();
                    assertEquals(((long[]) value).length, l.length);
                    for (int ix = 0; ix < ((long[]) value).length; ix++) {
                        assertEquals(((long[]) value)[ix], l[ix]);
                    }
                });
            else if (value instanceof String[])
                tryCommit(g, graph -> {
                    final String[] l = element.<String[]>property("aKey").value();
                    assertEquals(((String[]) value).length, l.length);
                    for (int ix = 0; ix < ((String[]) value).length; ix++) {
                        assertEquals(((String[]) value)[ix], l[ix]);
                    }
                });
            else
                tryCommit(g, graph -> assertEquals(value, element.property("aKey").value()));
        }

        private Edge createEdgeForPropertyFeatureTests() {
            final Vertex vertexA = g.addVertex();
            final Vertex vertexB = g.addVertex();
            return vertexA.addEdge(GraphManager.get().convertLabel("knows"), vertexB);
        }

        private Edge createEdgeForPropertyFeatureTests(final String k, Object v) {
            final Vertex vertexA = g.addVertex();
            final Vertex vertexB = g.addVertex();
            return vertexA.addEdge(GraphManager.get().convertLabel("knows"), vertexB, k, v);
        }
    }

    public static class MockSerializable implements Serializable {
        private String testField;

        public MockSerializable() {}

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
