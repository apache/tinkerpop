package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Optional;

import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_STRING_VALUES;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategyTest extends AbstractGremlinTest {
    private static final Optional<GraphStrategy> readOnlyGraphStrategy = Optional.<GraphStrategy>of(new ReadOnlyGraphStrategy());
    public ReadOnlyGraphStrategyTest() {
    }

    @Test
    public void shouldNotAllowAddVertex() {
        assertException(g::addVertex);
    }

    @Test
    public void shouldNotAllowRemoveVertex() {
        final Vertex v = g.addVertex();
        assertException(v::remove);
    }

    @Test
    public void shouldNotAllowAddEdge() {
        final Vertex v = g.addVertex();
        assertException(() -> v.addEdge("friend", v));
    }

    @Test
    public void shouldNotAllowRemoveEdge() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("friend", v);
        assertException(e::remove);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowVertexSetProperties() {
        final Vertex v = g.addVertex();
        assertException(()->v.setProperties("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowVertexSetProperty() {
        final Vertex v = g.addVertex();
        assertException(()->v.setProperty("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowEdgeSetProperties() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("friend", v);
        assertException(()->e.setProperties("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowEdgeSetProperty() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("friend", v);
        assertException(()->e.setProperty("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowVertexPropertyRemoval() {
        final Vertex v = g.addVertex();
        v.setProperty("test", "test");
        final Property<String> p = v.getProperty("test");
        assertEquals("test", p.get());
        assertException(p::remove);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowEdgePropertyRemoval() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("friend", v);
        e.setProperties("test", "test");
        final Property<String> p = e.getProperty("test");
        assertEquals("test", p.get());
        assertException(p::remove);
    }

    @Test
    public void shouldNotAllowGraphAnnotationSet() {
        assertException(() -> g.annotations().set("test", "fail"));
    }

    private void assertException(final SupplierThatThrows stt) {
        try {
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
            swg.strategy().setGraphStrategy(readOnlyGraphStrategy);
            stt.get();
        } catch (Exception ex) {
            final Exception expectedException = ReadOnlyGraphStrategy.Exceptions.graphUsesReadOnlyStrategy();
            assertEquals(expectedException.getClass(), ex.getClass());
            assertEquals(expectedException.getMessage(), ex.getMessage());

        }

    }

    @FunctionalInterface
    public interface SupplierThatThrows {
        public void get();
    }
}
