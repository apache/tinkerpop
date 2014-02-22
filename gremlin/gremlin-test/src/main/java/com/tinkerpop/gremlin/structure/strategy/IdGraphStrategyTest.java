package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Optional;
import java.util.UUID;

import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_STRING_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

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
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldInjectAnIdAndReturnBySpecifiedIdForVertex() {
            final IdGraphStrategy strategy = (IdGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
            final Vertex v = g.addVertex(Element.ID, "test", "something", "else");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertEquals("test", v.getId());
                assertEquals("test", v.getProperty(strategy.getIdKey()).get());
                assertEquals("else", v.getProperty("something").get());

                final Vertex found = g.v("test");
                assertEquals("test", found.getId());
                assertEquals("test", found.getProperty(strategy.getIdKey()).get());
                assertEquals("else", found.getProperty("something").get());

            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldInjectAnIdAndReturnBySpecifiedIdForEdge() {
            final IdGraphStrategy strategy = (IdGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
            final Vertex v = g.addVertex(Element.ID, "test", "something", "else");
            final Edge e = v.addEdge("self", v, Element.ID, "edge-id", "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertEquals("edge-id", e.getId());
                assertEquals("edge-id", e.getProperty(strategy.getIdKey()).get());
                assertEquals("this", e.getProperty("try").get());

                final Edge found = g.e("edge-id");
                assertEquals("edge-id", found.getId());
                assertEquals("edge-id", found.getProperty(strategy.getIdKey()).get());
                assertEquals("this", found.getProperty("try").get());
            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldCreateAnIdAndReturnByCreatedIdForVertex() {
            final IdGraphStrategy strategy = (IdGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
            final Vertex v = g.addVertex("something", "else");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertNotNull(UUID.fromString(v.getId().toString()));
                assertNotNull(UUID.fromString(v.getProperty(strategy.getIdKey()).get().toString()));
                assertEquals("else", v.getProperty("something").get());

                final Vertex found = g.v(v.getId());
                assertNotNull(UUID.fromString(found.getId().toString()));
                assertNotNull(UUID.fromString(found.getProperty(strategy.getIdKey()).get().toString()));
                assertEquals("else", found.getProperty("something").get());
            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldCreateAnIdAndReturnByCreatedIdForEdge() {
            final IdGraphStrategy strategy = (IdGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
            final Vertex v = g.addVertex("something", "else");
            final Edge e = v.addEdge("self", v, "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertNotNull(UUID.fromString(e.getId().toString()));
                assertNotNull(UUID.fromString(e.getProperty(strategy.getIdKey()).get().toString()));
                assertEquals("this", e.getProperty("try").get());

                final Edge found = g.e(e.getId());
                assertNotNull(UUID.fromString(found.getId().toString()));
                assertNotNull(UUID.fromString(found.getProperty(strategy.getIdKey()).get().toString()));
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
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldCreateAnIdAndReturnByCreatedId() {
            final IdGraphStrategy strategy = (IdGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
            final Vertex v = g.addVertex("something", "else");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertEquals("100", v.getId());
                assertEquals("100", v.getProperty(strategy.getIdKey()).get());
                assertEquals("else", v.getProperty("something").get());

                final Vertex found = g.v("100");
                assertEquals("100", found.getId());
                assertEquals("100", found.getProperty(strategy.getIdKey()).get());
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
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        public void shouldCreateAnIdAndReturnByCreatedId() {
            final IdGraphStrategy strategy = (IdGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
            final Vertex v = g.addVertex("something", "else");
            final Edge e = v.addEdge("self", v, "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertEquals("100", e.getId());
                assertEquals("100", e.getProperty(strategy.getIdKey()).get());
                assertEquals("this", e.getProperty("try").get());

                final Edge found = g.e("100");
                assertEquals("100", found.getId());
                assertEquals("100", found.getProperty(strategy.getIdKey()).get());
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
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        public void shouldInjectAnIdAndReturnBySpecifiedId() {
            final IdGraphStrategy strategy = (IdGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
            final Vertex v = g.addVertex(Element.ID, "test", "something", "else");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertEquals("test", v.getId());
                assertFalse(v.getProperty(strategy.getIdKey()).isPresent());
                assertEquals("else", v.getProperty("something").get());

                final Vertex found = g.v("test");
                assertEquals("test", found.getId());
                assertFalse(found.getProperty(strategy.getIdKey()).isPresent());
                assertEquals("else", found.getProperty("something").get());
            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        public void shouldAllowDirectSettingOfIdField() {
            final IdGraphStrategy strategy = (IdGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
            final Vertex v = g.addVertex(Element.ID, "test", "something", "else", strategy.getIdKey(), "should be ok to set this as supportsEdgeId=true");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertEquals("test", v.getId());
                assertEquals("should be ok to set this as supportsEdgeId=true", v.getProperty(strategy.getIdKey()).get());
                assertEquals("else", v.getProperty("something").get());

                final Vertex found = g.v("test");
                assertEquals("test", found.getId());
                assertEquals("should be ok to set this as supportsEdgeId=true", found.getProperty(strategy.getIdKey()).get());
                assertEquals("else", found.getProperty("something").get());
            });

            try {
                v.addEdge("self", v, Element.ID, "test", "something", "else", strategy.getIdKey(), "this should toss and exception as supportsVertexId=false");
                fail("An exception should be tossed here because supportsEdgeId=true");
            } catch (IllegalArgumentException iae) {
                assertNotNull(iae);
            }

            try {
                final Edge e = v.addEdge("self", v, Element.ID, "test", "something", "else");
                e.setProperty(strategy.getIdKey(), "this should toss and exception as supportsVertexId=false");
                fail("An exception should be tossed here because supportsEdgeId=true");
            } catch (IllegalArgumentException iae) {
                assertNotNull(iae);
            }
        }
    }

    public static class EdgeIdNotSupportedIdGraphStrategyTest extends AbstractGremlinTest {
        public EdgeIdNotSupportedIdGraphStrategyTest() {
            super(Optional.of(new IdGraphStrategy.Builder(idKey)
                    .supportsEdgeId(false).build()));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        public void shouldInjectAnIdAndReturnBySpecifiedId() {
            final IdGraphStrategy strategy = (IdGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
            final Vertex v = g.addVertex(Element.ID, "test", "something", "else");
            final Edge e = v.addEdge("self", v, Element.ID, "edge-id", "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertEquals("edge-id", e.getId());
                assertFalse(e.getProperty(strategy.getIdKey()).isPresent());
                assertEquals("this", e.getProperty("try").get());

                final Edge found = g.e("edge-id");
                assertEquals("edge-id", found.getId());
                assertFalse(found.getProperty(strategy.getIdKey()).isPresent());
                assertEquals("this", found.getProperty("try").get());
            });
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        public void shouldAllowDirectSettingOfIdField() {
            final IdGraphStrategy strategy = (IdGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
            final Vertex v = g.addVertex(Element.ID, "test", "something", "else");
            final Edge e = v.addEdge("self", v, Element.ID, "edge-id", "try", "this", strategy.getIdKey(), "should be ok to set this as supportsEdgeId=false");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertEquals("edge-id", e.getId());
                assertEquals("this", e.getProperty("try").get());
                assertEquals("should be ok to set this as supportsEdgeId=false", e.getProperty(strategy.getIdKey()).get());

                final Edge found = g.e("edge-id");
                assertEquals("edge-id", found.getId());
                assertEquals("this", found.getProperty("try").get());
                assertEquals("should be ok to set this as supportsEdgeId=false", found.getProperty(strategy.getIdKey()).get());
            });

            try {
                g.addVertex(Element.ID, "test", "something", "else", strategy.getIdKey(), "this should toss and exception as supportsVertexId=true");
                fail("An exception should be tossed here because supportsVertexId=true");
            } catch (IllegalArgumentException iae) {
                assertNotNull(iae);
            }

            try {
                v.setProperty(strategy.getIdKey(), "this should toss and exception as supportsVertexId=true");
                fail("An exception should be tossed here because supportsVertexId=true");
            } catch (IllegalArgumentException iae) {
                assertNotNull(iae);
            }
        }
    }
}
