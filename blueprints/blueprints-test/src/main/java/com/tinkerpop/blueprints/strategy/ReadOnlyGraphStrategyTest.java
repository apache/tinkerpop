package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.FeatureRequirement;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.Optional;

import static com.tinkerpop.blueprints.Graph.Features.GraphFeatures.FEATURE_STRATEGY;
import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_STRING_VALUES;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategyTest extends AbstractBlueprintsTest {
    private static final Optional<GraphStrategy> readOnlyGraphStrategy = Optional.<GraphStrategy>of(new ReadOnlyGraphStrategy());
    public ReadOnlyGraphStrategyTest() {
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    public void shouldNotAllowAddVertex() {
        assertException(g::addVertex);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    public void shouldNotAllowRemoveVertex() {
        final Vertex v = g.addVertex();
        assertException(v::remove);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    public void shouldNotAllowAddEdge() {
        final Vertex v = g.addVertex();
        assertException(() -> v.addEdge("friend", v));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    public void shouldNotAllowRemoveEdge() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("friend", v);
        assertException(e::remove);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowVertexSetProperties() {
        final Vertex v = g.addVertex();
        assertException(()->v.setProperties("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowVertexSetProperty() {
        final Vertex v = g.addVertex();
        assertException(()->v.setProperty("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowEdgeSetProperties() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("friend", v);
        assertException(()->e.setProperties("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowEdgeSetProperty() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("friend", v);
        assertException(()->e.setProperty("test", "test"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldNotAllowVertexPropertyRemoval() {
        final Vertex v = g.addVertex();
        v.setProperty("test", "test");
        final Property<String> p = v.getProperty("test");
        assertEquals("test", p.get());
        assertException(p::remove);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
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
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    public void shouldNotAllowGraphAnnotationSet() {
        assertException(() -> g.annotations().set("test", "fail"));
    }

    private void assertException(final SupplierThatThrows stt) {
        try {
            g.strategy().setGraphStrategy(readOnlyGraphStrategy);
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
