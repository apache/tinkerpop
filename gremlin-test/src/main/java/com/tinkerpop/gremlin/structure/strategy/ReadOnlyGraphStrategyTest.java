package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.*;
import org.junit.Test;

import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_STRING_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures.FEATURE_VARIABLES;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategyTest extends AbstractGremlinTest {
    private static final GraphStrategy readOnlyGraphStrategy = new ReadOnlyGraphStrategy();

    public ReadOnlyGraphStrategyTest() {
    }

    @Test
    public void shouldNotAllowAddVertex() {
        assertException(g -> g.addVertex());
    }

    @Test
    public void shouldNotAllowRemoveVertex() {
        g.addVertex();
        assertException(g -> g.V().next().remove());
    }

    @Test
    public void shouldNotAllowAddEdge() {
        g.addVertex();
        assertException(g -> {
            final Vertex v = g.V().next();
            v.addEdge("friend", v);
        });
    }

    @Test
    public void shouldNotAllowRemoveEdge() {
        final Vertex v = g.addVertex();
        v.addEdge("friend", v);
        assertException(g -> {
            final Edge e = g.E().next();
            e.remove();
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowVertexSetProperties() {
        g.addVertex();
        assertException(g -> g.V().next().properties("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowVertexSetProperty() {
        g.addVertex();
        assertException(g -> g.V().next().property("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowEdgeSetProperties() {
        final Vertex v = g.addVertex();
        v.addEdge("friend", v);
        assertException(g -> g.E().next().properties("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowEdgeSetProperty() {
        final Vertex v = g.addVertex();
        v.addEdge("friend", v);
        assertException(g -> g.E().next().property("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowVertexPropertyRemoval() {
        final Vertex v = g.addVertex();
        v.property("test", "test");
        final Property<String> p = v.property("test");
        assertEquals("test", p.value());

        assertException(g -> {
            final Property<String> prop = g.V().next().property("test");
            prop.remove();
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowEdgePropertyRemoval() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("friend", v);
        e.properties("test", "test");
        final Property<String> p = e.property("test");
        assertEquals("test", p.value());

        assertException(g -> {
            final Property<String> prop = g.E().next().property("test");
            prop.remove();
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = FEATURE_VARIABLES)
    public void shouldNotAllowVariableModifications() {
        assertException(g -> {
            g.variables().set("will", "not work");
        });
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = FEATURE_VARIABLES)
    public void shouldNotAllowVariableAsMapModifications() {
        g.variables().set("will", "be read-only");
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(readOnlyGraphStrategy);
        swg.variables().asMap().put("will", "not work");
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void testShouldReturnWrappedElementToString() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        Vertex v1 = swg.addVertex(Element.LABEL, "Person", "age", 1);
        Vertex v2 = swg.addVertex(Element.LABEL, "Person", "age", 1);
        Property age = v2.property("age");
        Edge e1 = v1.addEdge("friend", v2, "weight", "fifty");
        Vertex originalVertex = ((StrategyWrappedVertex) v1).getBaseVertex();
        Edge originalEdge = ((StrategyWrappedEdge) e1).getBaseEdge();
        Property originalProperty = originalVertex.property("age");
        assertEquals(originalVertex.toString(), v1.toString());
        assertEquals(originalEdge.toString(), e1.toString());
        assertEquals(originalProperty.toString(), age.toString());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void testGraphShouldReturnWrappedToString() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        assertNotEquals(g.toString(), swg.toString());
    }

    private void assertException(final ConsumerThatThrows stt) {
        try {
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
            swg.strategy().setGraphStrategy(readOnlyGraphStrategy);
            stt.accept(swg);
            fail();
        } catch (Exception ex) {
            final Exception expectedException = ReadOnlyGraphStrategy.Exceptions.graphUsesReadOnlyStrategy();
            assertEquals(expectedException.getClass(), ex.getClass());
            assertEquals(expectedException.getMessage(), ex.getMessage());

        }

    }

    @FunctionalInterface
    public interface ConsumerThatThrows {
        public void accept(final StrategyWrappedGraph graph) throws Exception;
    }
}
