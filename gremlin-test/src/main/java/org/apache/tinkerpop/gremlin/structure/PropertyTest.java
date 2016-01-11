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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.ExceptionCoverage;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * Gremlin Test Suite for {@link org.apache.tinkerpop.gremlin.structure.Property} operations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class PropertyTest {

    /**
     * Basic tests for the {@link org.apache.tinkerpop.gremlin.structure.Property} class.
     */
    public static class BasicPropertyTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldHaveStandardStringRepresentation() {
            final Vertex v = graph.addVertex("name", "marko");
            final Property p = v.property("name");
            assertEquals(StringFactory.propertyString(p), p.toString());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldReturnEmptyPropertyIfKeyNonExistent() {
            final Vertex v = graph.addVertex("name", "marko");
            tryCommit(graph, (graph) -> {
                final Vertex v1 = graph.vertices(v.id()).next();
                final VertexProperty p = v1.property("nonexistent-key");
                assertEquals(VertexProperty.empty(), p);
            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldAllowRemovalFromVertexWhenAlreadyRemoved() {
            final Vertex v = graph.addVertex("name", "marko");
            tryCommit(graph);
            final Vertex v1 = graph.vertices(v.id()).next();
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
            final Vertex v = graph.addVertex("name", "marko");
            tryCommit(graph);
            final Vertex v1 = graph.vertices(v.id()).next();

            try {
                final Edge edge = v1.addEdge("knows", graph.addVertex());
                final Property p = edge.property("stars", 5);
                p.remove();
                p.remove();
                edge.property("stars").remove();
                edge.property("stars").remove();
            } catch (Exception ex) {
                fail("Removing an edge property that was already removed should not throw an exception");
            }
        }
    }

    /**
     * Checks that properties added to an {@link org.apache.tinkerpop.gremlin.structure.Element} are validated in a consistent way when they are added at
     * {@link org.apache.tinkerpop.gremlin.structure.Vertex} or {@link org.apache.tinkerpop.gremlin.structure.Edge} construction by throwing an appropriate exception.
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

        @Parameterized.Parameters(name = "expect({0})")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"providedKeyValuesMustBeAMultipleOfTwo", new Object[]{"odd", "number", "arguments"}, Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    {"providedKeyValuesMustBeAMultipleOfTwo", new Object[]{"odd"}, Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    {"providedKeyValuesMustHaveALegalKeyOnEvenIndices", new Object[]{"odd", "number", 123, "test"}, Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    {"propertyValueCanNotBeNull", new Object[]{"odd", null}, Property.Exceptions.propertyValueCanNotBeNull()},
                    {"providedKeyValuesMustHaveALegalKeyOnEvenIndices", new Object[]{null, "val"}, Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    {"propertyKeyCanNotBeEmpty", new Object[]{"", "val"}, Property.Exceptions.propertyKeyCanNotBeEmpty()}});
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public Object[] arguments;

        @Parameterized.Parameter(value = 2)
        public Exception expectedException;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void shouldThrowOnGraphAddVertex() throws Exception {
            try {
                this.graph.addVertex(arguments);
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
                final Vertex v = this.graph.addVertex();
                v.addEdge("self", v, arguments);
                fail(String.format("Call to addVertex should have thrown an exception with these arguments [%s]", arguments));
            } catch (Exception ex) {
                validateException(expectedException, ex);
            }
        }
    }

    /**
     * Test exceptions around use of {@link org.apache.tinkerpop.gremlin.structure.Element#value(String)}.
     */
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyDoesNotExist"
    })
    public static class ElementGetValueExceptionConsistencyTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void shouldGetValueThatIsNotPresentOnVertex() {
            final Vertex v = graph.addVertex();
            try {
                v.value("does-not-exist");
                fail("Call to Element.value() with a key that is not present should throw an exception");
            } catch (Exception ex) {
                validateException(Property.Exceptions.propertyDoesNotExist(v, "does-not-exist"), ex);
            }

        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void shouldGetValueThatIsNotPresentOnEdge() {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("self", v);
            try {
                e.value("does-not-exist");
                fail("Call to Element.value() with a key that is not present should throw an exception");
            } catch (Exception ex) {
                validateException(Property.Exceptions.propertyDoesNotExist(e, "does-not-exist"), ex);
            }

        }
    }


    /**
     * Checks that properties added to an {@link org.apache.tinkerpop.gremlin.structure.Element} are validated in a
     * consistent way when they are set after {@link Vertex} or {@link Edge} construction by throwing an
     * appropriate exception.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyValueCanNotBeNull",
            "propertyKeyCanNotBeNull",
            "propertyKeyCanNotBeEmpty",
            "propertyKeyCanNotBeAHiddenKey"
    })
    public static class PropertyValidationOnSetExceptionConsistencyTest extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "expect({0})")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"propertyValueCanNotBeNull", "k", null, Property.Exceptions.propertyValueCanNotBeNull()},
                    {"propertyKeyCanNotBeNull", null, "v", Property.Exceptions.propertyKeyCanNotBeNull()},
                    {"propertyKeyCanNotBeEmpty", "", "v", Property.Exceptions.propertyKeyCanNotBeEmpty()},
                    {"propertyKeyCanNotBeAHiddenKey", Graph.Hidden.hide("systemKey"), "value", Property.Exceptions.propertyKeyCanNotBeAHiddenKey(Graph.Hidden.hide("systemKey"))}});
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public String key;

        @Parameterized.Parameter(value = 2)
        public String val;

        @Parameterized.Parameter(value = 3)
        public Exception expectedException;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = VertexPropertyFeatures.FEATURE_ADD_PROPERTY)
        public void testGraphVertexSetPropertyStandard() throws Exception {
            try {
                final Vertex v = this.graph.addVertex();
                v.property(VertexProperty.Cardinality.single, key, val);
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
                final Vertex v = this.graph.addVertex();
                v.addEdge("self", v).property(key, val);
                fail(String.format("Call to Edge.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                validateException(expectedException, ex);
            }
        }
    }

    /**
     * Tests for feature support on {@link org.apache.tinkerpop.gremlin.structure.Property}.  The tests validate if {@link org.apache.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures}
     * should be turned on or off and if the enabled features are properly supported by the implementation.  Note that
     * these tests are run in a separate test class as they are "parameterized" tests.
     */
    @RunWith(Parameterized.class)
    public static class PropertyFeatureSupportTest extends AbstractGremlinTest {
        private static final Map<String, Object> testMap = new HashMap<>();

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

        @Parameterized.Parameters(name = "supports{0}({1})")
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
            assumeThat(graph.features().supports(EdgePropertyFeatures.class, featureName), is(true));
            final Edge edge = createEdgeForPropertyFeatureTests();
            edge.property("aKey", value);
            assertPropertyValue(edge);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
        public void shouldSetValueOnVertex() throws Exception {
            assumeThat(graph.features().supports(VertexPropertyFeatures.class, featureName), is(true));
            final Vertex vertex = graph.addVertex();
            vertex.property(VertexProperty.Cardinality.single, "aKey", value);
            assertPropertyValue(vertex);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldSetValueOnEdgeOnAdd() throws Exception {
            assumeThat(graph.features().supports(EdgePropertyFeatures.class, featureName), is(true));
            final Edge edge = createEdgeForPropertyFeatureTests("aKey", value);
            assertPropertyValue(edge);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldSetValueOnVertexOnAdd() throws Exception {
            assumeThat(graph.features().supports(VertexPropertyFeatures.class, featureName), is(true));
            final Vertex vertex = graph.addVertex("aKey", value);
            assertPropertyValue(vertex);
        }

        private void assertPropertyValue(final Element element) {
            if (value instanceof Map)
                tryCommit(graph, graph -> {
                    final Map map = element.<Map>property("aKey").value();
                    assertEquals(((Map) value).size(), map.size());
                    ((Map) value).keySet().forEach(k -> assertEquals(((Map) value).get(k), map.get(k)));
                });
            else if (value instanceof List)
                tryCommit(graph, graph -> {
                    final List l = element.<List>property("aKey").value();
                    assertEquals(((List) value).size(), l.size());
                    for (int ix = 0; ix < ((List) value).size(); ix++) {
                        assertEquals(((List) value).get(ix), l.get(ix));
                    }
                });
            else if (value instanceof MockSerializable)
                tryCommit(graph, graph -> {
                    final MockSerializable mock = element.<MockSerializable>property("aKey").value();
                    assertEquals(((MockSerializable) value).getTestField(), mock.getTestField());
                });
            else if (value instanceof boolean[])
                tryCommit(graph, graph -> {
                    final boolean[] l = element.<boolean[]>property("aKey").value();
                    assertEquals(((boolean[]) value).length, l.length);
                    for (int ix = 0; ix < ((boolean[]) value).length; ix++) {
                        assertEquals(((boolean[]) value)[ix], l[ix]);
                    }
                });
            else if (value instanceof double[])
                tryCommit(graph, graph -> {
                    final double[] l = element.<double[]>property("aKey").value();
                    assertEquals(((double[]) value).length, l.length);
                    for (int ix = 0; ix < ((double[]) value).length; ix++) {
                        assertEquals(((double[]) value)[ix], l[ix], 0.0d);
                    }
                });
            else if (value instanceof float[])
                tryCommit(graph, graph -> {
                    final float[] l = element.<float[]>property("aKey").value();
                    assertEquals(((float[]) value).length, l.length);
                    for (int ix = 0; ix < ((float[]) value).length; ix++) {
                        assertEquals(((float[]) value)[ix], l[ix], 0.0f);
                    }
                });
            else if (value instanceof int[])
                tryCommit(graph, graph -> {
                    final int[] l = element.<int[]>property("aKey").value();
                    assertEquals(((int[]) value).length, l.length);
                    for (int ix = 0; ix < ((int[]) value).length; ix++) {
                        assertEquals(((int[]) value)[ix], l[ix]);
                    }
                });
            else if (value instanceof long[])
                tryCommit(graph, graph -> {
                    final long[] l = element.<long[]>property("aKey").value();
                    assertEquals(((long[]) value).length, l.length);
                    for (int ix = 0; ix < ((long[]) value).length; ix++) {
                        assertEquals(((long[]) value)[ix], l[ix]);
                    }
                });
            else if (value instanceof String[])
                tryCommit(graph, graph -> {
                    final String[] l = element.<String[]>property("aKey").value();
                    assertEquals(((String[]) value).length, l.length);
                    for (int ix = 0; ix < ((String[]) value).length; ix++) {
                        assertEquals(((String[]) value)[ix], l[ix]);
                    }
                });
            else
                tryCommit(graph, graph -> assertEquals(value, element.property("aKey").value()));
        }

        private Edge createEdgeForPropertyFeatureTests() {
            final Vertex vertexA = graph.addVertex();
            final Vertex vertexB = graph.addVertex();
            return vertexA.addEdge(graphProvider.convertLabel("knows"), vertexB);
        }

        private Edge createEdgeForPropertyFeatureTests(final String k, Object v) {
            final Vertex vertexA = graph.addVertex();
            final Vertex vertexB = graph.addVertex();
            return vertexA.addEdge(graphProvider.convertLabel("knows"), vertexB, k, v);
        }
    }
}
