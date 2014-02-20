package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Optional;
import java.util.UUID;

import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_STRATEGY;
import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_STRING_VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class IdGraphStrategyTest {
    private static final String idKey = "myId";

    public static class DefaultIdGraphStrategyTest extends AbstractGremlinTest {
        public DefaultIdGraphStrategyTest() {
            super(Optional.of(new IdGraphStrategy.Builder(idKey).build()));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldInjectAnIdAndReturnBySpecifiedIdForVertex() {
            final Vertex v = g.addVertex(Element.ID, "test", "something", "else");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertEquals("test", v.getProperty(Property.Key.hidden(idKey)).get());
                assertEquals("else", v.getProperty("something").get());

                final Vertex found = g.v("test");
                assertEquals(v, found);
                assertEquals("test", found.getProperty(Property.Key.hidden(idKey)).get());
                assertEquals("else", found.getProperty("something").get());

            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldInjectAnIdAndReturnBySpecifiedIdForEdge() {
            final Vertex v = g.addVertex(Element.ID, "test", "something", "else");
            final Edge e = v.addEdge("self", v, Element.ID, "edge-id", "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertEquals("edge-id", e.getProperty(Property.Key.hidden(idKey)).get());
                assertEquals("this", e.getProperty("try").get());

                final Edge found = g.e("edge-id");
                assertEquals(e, found);
                assertEquals("edge-id", found.getProperty(Property.Key.hidden(idKey)).get());
                assertEquals("this", found.getProperty("try").get());
            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldCreateAnIdAndReturnByCreatedIdForVertex() {
            final Vertex v = g.addVertex("something", "else");

            tryCommit(g, c -> {
                assertNotNull(v);
                assertNotNull(UUID.fromString(v.getProperty(Property.Key.hidden(idKey)).get().toString()));
                assertEquals("else", v.getProperty("something").get());

                final Vertex found = g.v(v.getId());
                assertEquals(v, found);
                assertNotNull(UUID.fromString(found.getProperty(Property.Key.hidden(idKey)).get().toString()));
                assertEquals("else", found.getProperty("something").get());

            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldCreateAnIdAndReturnByCreatedIdForEdge() {
            final Vertex v = g.addVertex("something", "else");
            final Edge e = v.addEdge("self", v, "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertNotNull(UUID.fromString(e.getProperty(Property.Key.hidden(idKey)).get().toString()));
                assertEquals("this", e.getProperty("try").get());

                final Edge found = g.e(e.getId());
                assertEquals(e, found);
                assertNotNull(UUID.fromString(found.getProperty(Property.Key.hidden(idKey)).get().toString()));
                assertEquals("this", found.getProperty("try").get());
            });
        }
    }

    public static class VertexIdMakerIdGraphStrategyTest extends AbstractGremlinTest {
        public VertexIdMakerIdGraphStrategyTest() {
            super(Optional.of(new IdGraphStrategy.Builder(idKey)
                    .vertexIdMaker(() -> "100").build()));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldCreateAnIdAndReturnByCreatedId() {
            final Vertex v = g.addVertex("something", "else");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertEquals("100", v.getProperty(Property.Key.hidden(idKey)).get());
                assertEquals("else", v.getProperty("something").get());

                final Vertex found = g.v("100");
                assertEquals(v, found);
                assertEquals("100", found.getProperty(Property.Key.hidden(idKey)).get());
                assertEquals("else", found.getProperty("something").get());

            });
        }
    }

    public static class EdgeIdMakerIdGraphStrategyTest extends AbstractGremlinTest {
        public EdgeIdMakerIdGraphStrategyTest() {
            super(Optional.of(new IdGraphStrategy.Builder(idKey)
                    .edgeIdMaker(() -> "100").build()));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldCreateAnIdAndReturnByCreatedId() {
            final Vertex v = g.addVertex("something", "else");
            final Edge e = v.addEdge("self", v, "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertEquals("100", e.getProperty(Property.Key.hidden(idKey)).get());
                assertEquals("this", e.getProperty("try").get());

                final Edge found = g.e("100");
                assertEquals(e, found);
                assertEquals("100", found.getProperty(Property.Key.hidden(idKey)).get());
                assertEquals("this", found.getProperty("try").get());
            });
        }
    }

    public static class VertexIdNotSupportedIdGraphStrategyTest extends AbstractGremlinTest {
        public VertexIdNotSupportedIdGraphStrategyTest() {
            super(Optional.of(new IdGraphStrategy.Builder(idKey)
                    .supportsVertexId(false).build()));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldInjectAnIdAndReturnBySpecifiedId() {
            final Vertex v = g.addVertex(Element.ID, "test", "something", "else");

            tryCommit(g, c -> {
                assertNotNull(v);

                if (g.getFeatures().vertex().supportsUserSuppliedIds())
                    assertEquals("test", v.getId());

                assertFalse(v.getProperty(Property.Key.hidden(idKey)).isPresent());
                assertEquals("else", v.getProperty("something").get());

                final Vertex found = g.v("test");
                assertEquals(v, found);
                if (g.getFeatures().vertex().supportsUserSuppliedIds())
                    assertEquals("test", v.getId());

                assertFalse(v.getProperty(Property.Key.hidden(idKey)).isPresent());
                assertEquals("else", found.getProperty("something").get());

            });
        }
    }

    // todo: test edge creation still
}
