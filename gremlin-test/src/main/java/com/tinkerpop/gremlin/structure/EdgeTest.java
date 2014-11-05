package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.ExceptionCoverage;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.function.FunctionUtils;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;

import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.*;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Enclosed.class)
public class EdgeTest {

    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "labelCanNotBeNull",
            "labelCanNotBeEmpty",
            "labelCanNotBeASystemKey"
    })
    public static class BasicVertexTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveStandardStringRepresentation() {
            final Vertex v1 = g.addVertex();
            final Vertex v2 = g.addVertex();
            final Edge e = v1.addEdge("friends", v2);

            assertEquals(StringFactory.edgeString(e), e.toString());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingNullEdgeLabel() {
            final Vertex v = g.addVertex();
            try {
                v.addEdge(null, v);
                fail("Call to Vertex.addEdge() should throw an exception when label is null");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeNull(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingEmptyVertexLabel() {
            final Vertex v = g.addVertex();
            try {
                v.addEdge("", v);
                fail("Call to Vertex.addEdge() should throw an exception when label is empty");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeEmpty(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveExceptionConsistencyWhenUsingSystemVertexLabel() {
            final String label = Graph.System.system("systemLabel");
            final Vertex v = g.addVertex();
            try {
                v.addEdge(label, v);
                fail("Call to Vertex.addEdge() should throw an exception when label is a system key");
            } catch (Exception ex) {
                validateException(Element.Exceptions.labelCanNotBeASystemKey(label), ex);
            }
        }

        @Test(expected = NoSuchElementException.class)
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldThrowNoSuchElementExceptionIfEdgeWithIdNotPresent() {
            g.e("this-id-should-not-be-in-the-modern-graph");
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldAutotypeStringProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "string", "marko");
            final String name = e.value("string");
            assertEquals(name, "marko");

        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
        public void shouldAutotypIntegerProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "integer", 33);
            final Integer age = e.value("integer");
            assertEquals(Integer.valueOf(33), age);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_BOOLEAN_VALUES)
        public void shouldAutotypeBooleanProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "boolean", true);
            final Boolean best = e.value("boolean");
            assertEquals(best, true);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_DOUBLE_VALUES)
        public void shouldAutotypeDoubleProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "double", 0.1d);
            final Double best = e.value("double");
            assertEquals(best, Double.valueOf(0.1d));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_LONG_VALUES)
        public void shouldAutotypeLongProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "long", 1l);
            final Long best = e.value("long");
            assertEquals(best, Long.valueOf(1l));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
        public void shouldAutotypeFloatProperties() {
            final Graph graph = g;
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v, "float", 0.1f);
            final Float best = e.value("float");
            assertEquals(best, Float.valueOf(0.1f));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldGetPropertyKeysOnEdge() {
            final Vertex v = g.addVertex();
            final Edge e = v.addEdge("friend", v, "name", "marko", "location", "desert", "status", "dope");
            Set<String> keys = e.keys();
            assertEquals(3, keys.size());

            assertTrue(keys.contains("name"));
            assertTrue(keys.contains("location"));
            assertTrue(keys.contains("status"));

            final List<Property<Object>> m = e.properties().toList();
            assertEquals(3, m.size());
            assertTrue(m.stream().anyMatch(p -> p.key().equals("name")));
            assertTrue(m.stream().anyMatch(p -> p.key().equals("location")));
            assertTrue(m.stream().anyMatch(p -> p.key().equals("status")));
            assertEquals("marko", m.stream().filter(p -> p.key().equals("name")).map(Property::value).findAny().orElse(null));
            assertEquals("desert", m.stream().filter(p -> p.key().equals("location")).map(Property::value).findAny().orElse(null));
            assertEquals("dope", m.stream().filter(p -> p.key().equals("status")).map(Property::value).findAny().orElse(null));

            e.property("status").remove();

            keys = e.keys();
            assertEquals(2, keys.size());
            assertTrue(keys.contains("name"));
            assertTrue(keys.contains("location"));

            e.properties().remove();

            keys = e.keys();
            assertEquals(0, keys.size());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_REMOVE_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldNotGetConcurrentModificationException() {
            for (int i = 0; i < 25; i++) {
                final Vertex v = g.addVertex();
                v.addEdge("friend", v);
            }

            tryCommit(g, assertVertexEdgeCounts(25, 25));

            for (Edge e : g.E().toList()) {
                e.remove();
                tryCommit(g);
            }

            tryCommit(g, assertVertexEdgeCounts(25, 0));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldReturnEmptyIteratorIfNoProperties() {
            final Vertex v = g.addVertex();
            final Edge e = v.addEdge("knows", v);
            assertEquals(0, e.properties().count().next().intValue());
        }
    }

    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "elementAlreadyRemoved"
    })
    public static class ExceptionConsistencyWhenEdgeRemovedTest extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{index}: expect - {0}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"property(k)", FunctionUtils.wrapConsumer((Edge e) -> e.property("x"))},
                    {"e.remove()", FunctionUtils.wrapConsumer(Edge::remove)}});
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public Consumer<Edge> functionToTest;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
        public void shouldThrowExceptionIfEdgeWasRemoved() {
            final Vertex v1 = g.addVertex("name", "stephen");
            final Edge e = v1.addEdge("knows", v1, "x", "y");
            final Object id = e.id();
            e.remove();
            tryCommit(g, g -> {
                try {
                    functionToTest.accept(e);
                    fail("Should have thrown exception as the Edge was already removed");
                } catch (Exception ex) {
                    validateException(Element.Exceptions.elementAlreadyRemoved(Edge.class, id), ex);
                }
            });
        }
    }
}
