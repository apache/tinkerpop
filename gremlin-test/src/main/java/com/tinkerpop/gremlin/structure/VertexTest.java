package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.ExceptionCoverage;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.function.FunctionUtils;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;

import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.*;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class VertexTest {

    @ExceptionCoverage(exceptionClass = Edge.Exceptions.class, methods = {
            "userSuppliedIdsNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
            "edgeWithIdAlreadyExists"
    })
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "labelCanNotBeNull",
            "labelCanNotBeEmpty",
            "labelCanNotBeASystemKey"
    })
    public static class BasicVertexTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingNullVertexLabel() {
            try {
                g.addVertex(T.label, null);
                fail("Call to Graph.addVertex() should throw an exception when label is null");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeNull(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingNullVertexLabelOnOverload() {
            try {
                g.addVertex((String) null);
                fail("Call to Graph.addVertex() should throw an exception when label is null");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeNull(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingEmptyVertexLabel() {
            try {
                g.addVertex(T.label, "");
                fail("Call to Graph.addVertex() should throw an exception when label is empty");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeEmpty(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingEmptyVertexLabelOnOverload() {
            try {
                g.addVertex("");
                fail("Call to Graph.addVertex() should throw an exception when label is empty");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeEmpty(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingSystemVertexLabel() {
            final String label = Graph.System.system("systemLabel");
            try {
                g.addVertex(T.label, label);
                fail("Call to Graph.addVertex() should throw an exception when label is a system key");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeASystemKey(label), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingSystemVertexLabelOnOverload() {
            final String label = Graph.System.system("systemLabel");
            try {
                g.addVertex(label);
                fail("Call to Graph.addVertex() should throw an exception when label is a system key");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeASystemKey(label), ex);
            }
        }

        @Test(expected = NoSuchElementException.class)
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldThrowNoSuchElementExceptionIfVertexWithIdNotPresent() {
            g.v("this-id-should-not-be-in-the-modern-graph");
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_NUMERIC_IDS)
        public void shouldHaveExceptionConsistencyWhenAssigningSameIdOnEdge() {
            final Vertex v = g.addVertex();
            final Object o = GraphManager.get().convertId("1");
            v.addEdge("label", v, T.id, o);

            try {
                v.addEdge("label", v, T.id, o);
                fail("Assigning the same ID to an Element should throw an exception");
            } catch (Exception ex) {
                validateException(Graph.Exceptions.edgeWithIdAlreadyExists(o), ex);
            }

        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void shouldHaveExceptionConsistencyWhenIdNotSupportedForAddEdge() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v, T.id, "");
                fail("Call to addEdge should have thrown an exception when ID was specified as it is not supported");
            } catch (Exception ex) {
                validateException(Edge.Exceptions.userSuppliedIdsNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        public void shouldHaveStandardStringRepresentation() {
            final Vertex v = g.addVertex("name", "marko", "age", 34);
            assertEquals(StringFactory.vertexString(v), v.toString());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldUseDefaultLabelIfNotSpecified() {
            final Vertex v = g.addVertex("name", "marko");
            assertEquals(Vertex.DEFAULT_LABEL, v.label());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldAddVertexWithLabel() {
            final Vertex v = g.addVertex("person");
            this.tryCommit(g, graph -> assertEquals("person", v.label()));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldSupportBasicVertexManipulation() {
            // test property mutation behaviors
            final Vertex v = g.addVertex("name", "marko", "age", 34);
            assertEquals(34, (int) v.value("age"));
            assertEquals("marko", v.<String>value("name"));
            assertEquals(34, (int) v.property("age").value());
            assertEquals("marko", v.<String>property("name").value());
            assertEquals(2, v.properties().count().next().intValue());
            assertEquals(2, v.keys().size());
            assertTrue(v.keys().contains("name"));
            assertTrue(v.keys().contains("age"));
            assertFalse(v.keys().contains("location"));
            assertVertexEdgeCounts(1, 0).accept(g);

            v.properties("name").remove();
            v.property("name", "marko rodriguez");
            assertEquals(34, (int) v.value("age"));
            assertEquals("marko rodriguez", v.<String>value("name"));
            assertEquals(34, (int) v.property("age").value());
            assertEquals("marko rodriguez", v.<String>property("name").value());
            assertEquals(2, v.properties().count().next().intValue());
            assertEquals(2, v.keys().size());
            assertTrue(v.keys().contains("name"));
            assertTrue(v.keys().contains("age"));
            assertFalse(v.keys().contains("location"));
            assertVertexEdgeCounts(1, 0).accept(g);

            v.property("location", "santa fe");
            assertEquals(3, v.properties().count().next().intValue());
            assertEquals(3, v.keys().size());
            assertEquals("santa fe", v.property("location").value());
            assertEquals(v.property("location"), v.property("location"));
            assertNotEquals(v.property("location"), v.property("name"));
            assertTrue(v.keys().contains("name"));
            assertTrue(v.keys().contains("age"));
            assertTrue(v.keys().contains("location"));
            v.property("location").remove();
            assertVertexEdgeCounts(1, 0).accept(g);
            assertEquals(2, v.properties().count().next().intValue());
            v.properties().remove();
            assertEquals(0, v.properties().count().next().intValue());
            assertVertexEdgeCounts(1, 0).accept(g);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        public void shouldEvaluateVerticesEquivalentWithSuppliedIds() {
            final Vertex v = g.addVertex(T.id, GraphManager.get().convertId("1"));
            final Vertex u = g.v(GraphManager.get().convertId("1"));
            assertEquals(v, u);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldEvaluateEquivalentVerticesWithNoSuppliedIds() {
            final Vertex v = g.addVertex();
            assertNotNull(v);

            final Vertex u = g.v(v.id());
            assertNotNull(u);
            assertEquals(v, u);

            assertEquals(g.v(u.id()), g.v(u.id()));
            assertEquals(g.v(v.id()), g.v(u.id()));
            assertEquals(g.v(v.id()), g.v(v.id()));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        public void shouldEvaluateEquivalentVertexHashCodeWithSuppliedIds() {
            final Vertex v = g.addVertex(T.id, GraphManager.get().convertId("1"));
            final Vertex u = g.v(GraphManager.get().convertId("1"));
            assertEquals(v, u);

            final Set<Vertex> set = new HashSet<>();
            set.add(v);
            set.add(v);
            set.add(u);
            set.add(u);
            set.add(g.v(GraphManager.get().convertId("1")));
            set.add(g.v(GraphManager.get().convertId("1")));

            assertEquals(1, set.size());
            assertEquals(v.hashCode(), u.hashCode());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldAutotypeStringProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            v.property("string", "marko");
            final String name = v.value("string");
            assertEquals(name, "marko");

        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        public void shouldAutotypIntegerProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            v.property("integer", 33);
            final Integer age = v.value("integer");
            assertEquals(Integer.valueOf(33), age);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_BOOLEAN_VALUES)
        public void shouldAutotypeBooleanProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            v.property("boolean", true);
            final Boolean best = v.value("boolean");
            assertEquals(best, true);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_DOUBLE_VALUES)
        public void shouldAutotypeDoubleProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            v.property("double", 0.1d);
            final Double best = v.value("double");
            assertEquals(best, Double.valueOf(0.1d));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_LONG_VALUES)
        public void shouldAutotypeLongProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            v.property("long", 1l);
            final Long best = v.value("long");
            assertEquals(best, Long.valueOf(1l));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
        public void shouldAutotypeFloatProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            v.property("float", 0.1f);
            final Float best = v.value("float");
            assertEquals(best, Float.valueOf(0.1f));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldGetPropertyKeysOnVertex() {
            final Vertex v = g.addVertex("name", "marko", "location", "desert", "status", "dope");
            Set<String> keys = v.keys();
            assertEquals(3, keys.size());

            assertTrue(keys.contains("name"));
            assertTrue(keys.contains("location"));
            assertTrue(keys.contains("status"));

            final List<VertexProperty<Object>> m = v.properties().toList();
            assertEquals(3, m.size());
            assertTrue(m.stream().anyMatch(p -> p.key().equals("name")));
            assertTrue(m.stream().anyMatch(p -> p.key().equals("location")));
            assertTrue(m.stream().anyMatch(p -> p.key().equals("status")));
            assertEquals("marko", m.stream().filter(p -> p.key().equals("name")).map(Property::value).findAny().orElse(null));
            assertEquals("desert", m.stream().filter(p -> p.key().equals("location")).map(Property::value).findAny().orElse(null));
            assertEquals("dope", m.stream().filter(p -> p.key().equals("status")).map(Property::value).findAny().orElse(null));

            v.property("status").remove();

            keys = v.keys();
            assertEquals(2, keys.size());
            assertTrue(keys.contains("name"));
            assertTrue(keys.contains("location"));

            v.properties().remove();

            keys = v.keys();
            assertEquals(0, keys.size());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
        public void shouldNotGetConcurrentModificationException() {
            for (int i = 0; i < 25; i++) {
                g.addVertex("myId", i);
            }
            g.V().forEachRemaining(v -> g.V().forEachRemaining(u -> v.addEdge("knows", u, "myEdgeId", 12)));

            tryCommit(g, assertVertexEdgeCounts(25, 625));

            for (Vertex v : g.V().toList()) {
                v.remove();
                tryCommit(g);
            }

            tryCommit(g, assertVertexEdgeCounts(0, 0));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldReturnEmptyIteratorIfNoProperties() {
            final Vertex v = g.addVertex();
            assertEquals(0, v.properties().count().next().intValue());
        }
    }

    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "elementAlreadyRemoved"
    })
    public static class ExceptionConsistencyWhenVertexRemovedTest extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{index}: expect - {0}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"property(k)", FunctionUtils.wrapConsumer((Vertex v) -> v.property("name"))},
                    {"v.remove()", FunctionUtils.wrapConsumer(Vertex::remove)}});
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public Consumer<Vertex> functionToTest;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
        public void shouldThrowExceptionIfVertexWasRemovedWhenCallingProperty() {
            final Vertex v1 = g.addVertex("name", "stephen");
            final Object id = v1.id();
            v1.remove();
            tryCommit(g, g -> {
                try {
                    functionToTest.accept(v1);
                    fail("Should have thrown exception as the Vertex was already removed");
                } catch (Exception ex) {
                    validateException(Element.Exceptions.elementAlreadyRemoved(Vertex.class, id), ex);
                }
            });
        }
    }
}
