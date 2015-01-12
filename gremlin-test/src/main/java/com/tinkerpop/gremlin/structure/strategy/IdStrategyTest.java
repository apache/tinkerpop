package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class IdStrategyTest {
    private static final String idKey = "myId";

    public static class DefaultIdStrategyTest extends AbstractGremlinTest {

        public DefaultIdStrategyTest() {
            super(IdStrategy.build(idKey).create());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldInjectAnIdAndReturnBySpecifiedIdForVertex() {
            final IdStrategy strategy = (IdStrategy) ((StrategyGraph) g).getStrategy();
            final Vertex v = g.addVertex(T.id, "test", "something", "else");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertEquals("test", v.id());
                assertEquals("test", v.property(strategy.getIdKey()).value());
                assertEquals("else", v.property("something").value());

                final Vertex found = g.V("test").next();
                assertEquals("test", found.id());
                assertEquals("test", found.property(strategy.getIdKey()).value());
                assertEquals("else", found.property("something").value());

            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldInjectAnIdAndReturnBySpecifiedIdForEdge() {
            final IdStrategy strategy = (IdStrategy) ((StrategyGraph) g).getStrategy();
            final Vertex v = g.addVertex(T.id, "test", "something", "else");
            final Edge e = v.addEdge("self", v, T.id, "edge-id", "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertEquals("edge-id", e.id());
                assertEquals("edge-id", e.property(strategy.getIdKey()).value());
                assertEquals("this", e.property("try").value());

                final Edge found = g.E("edge-id").next();
                assertEquals("edge-id", found.id());
                assertEquals("edge-id", found.property(strategy.getIdKey()).value());
                assertEquals("this", found.property("try").value());
            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldCreateAnIdAndReturnByCreatedIdForVertex() {
            final IdStrategy strategy = (IdStrategy) ((StrategyGraph) g).getStrategy();
            final Vertex v = g.addVertex("something", "else");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertNotNull(UUID.fromString(v.id().toString()));
                assertNotNull(UUID.fromString(v.property(strategy.getIdKey()).value().toString()));
                assertEquals("else", v.property("something").value());

                final Vertex found = g.V(v.id()).next();
                assertNotNull(UUID.fromString(found.id().toString()));
                assertNotNull(UUID.fromString(found.property(strategy.getIdKey()).value().toString()));
                assertEquals("else", found.property("something").value());
            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldCreateAnIdAndReturnByCreatedIdForEdge() {
            final IdStrategy strategy = (IdStrategy) ((StrategyGraph) g).getStrategy();
            final Vertex v = g.addVertex("something", "else");
            final Edge e = v.addEdge("self", v, "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertNotNull(UUID.fromString(e.id().toString()));
                assertNotNull(UUID.fromString(e.property(strategy.getIdKey()).value().toString()));
                assertEquals("this", e.property("try").value());

                final Edge found = g.E(e.id()).next();
                assertNotNull(UUID.fromString(found.id().toString()));
                assertNotNull(UUID.fromString(found.property(strategy.getIdKey()).value().toString()));
                assertEquals("this", found.property("try").value());
            });
        }
    }

    public static class VertexIdMakerIdStrategyTest extends AbstractGremlinTest {
        public VertexIdMakerIdStrategyTest() {
            super(IdStrategy.build(idKey).vertexIdMaker(() -> "100").create());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldCreateAnIdAndReturnByCreatedId() {
            final IdStrategy strategy = (IdStrategy) ((StrategyGraph) g).getStrategy();
            final Vertex v = g.addVertex("something", "else");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertEquals("100", v.id());
                assertEquals("100", v.property(strategy.getIdKey()).value());
                assertEquals("else", v.property("something").value());

                final Vertex found = g.V("100").next();
                assertEquals("100", found.id());
                assertEquals("100", found.property(strategy.getIdKey()).value());
                assertEquals("else", found.property("something").value());

            });
        }
    }

    public static class EdgeIdMakerIdStrategyTest extends AbstractGremlinTest {
        public EdgeIdMakerIdStrategyTest() {
            super(IdStrategy.build(idKey).edgeIdMaker(() -> "100").create());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldCreateAnIdAndReturnByCreatedId() {
            final IdStrategy strategy = (IdStrategy) ((StrategyGraph) g).getStrategy();
            final Vertex v = g.addVertex("something", "else");
            final Edge e = v.addEdge("self", v, "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertEquals("100", e.id());
                assertEquals("100", e.property(strategy.getIdKey()).value());
                assertEquals("this", e.property("try").value());

                final Edge found = g.E("100").next();
                assertEquals("100", found.id());
                assertEquals("100", found.property(strategy.getIdKey()).value());
                assertEquals("this", found.property("try").value());
            });
        }
    }

    public static class VertexIdNotSupportedIdStrategyTest extends AbstractGremlinTest {
        public VertexIdNotSupportedIdStrategyTest() {
            super(IdStrategy.build(idKey).supportsVertexId(false).create());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        public void shouldInjectAnIdAndReturnBySpecifiedId() {
            final IdStrategy strategy = (IdStrategy) ((StrategyGraph) g).getStrategy();
            final Object o = GraphManager.get().convertId("1");
            final Vertex v = g.addVertex(T.id, o, "something", "else");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertEquals(o, v.id());
                assertFalse(v.property(strategy.getIdKey()).isPresent());
                assertEquals("else", v.property("something").value());

                final Vertex found = g.V(o).next();
                assertEquals(o, found.id());
                assertFalse(found.property(strategy.getIdKey()).isPresent());
                assertEquals("else", found.property("something").value());
            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        public void shouldAllowDirectSettingOfIdField() {
            final IdStrategy strategy = (IdStrategy) ((StrategyGraph) g).getStrategy();
            final Object o = GraphManager.get().convertId("1");
            final Vertex v = g.addVertex(T.id, o, "something", "else", strategy.getIdKey(), "should be ok to set this as supportsEdgeId=true");
            tryCommit(g, c -> {
                assertNotNull(v);
                assertEquals(o, v.id());
                assertEquals("should be ok to set this as supportsEdgeId=true", v.property(strategy.getIdKey()).value());
                assertEquals("else", v.property("something").value());

                final Vertex found = g.V(o).next();
                assertEquals(o, found.id());
                assertEquals("should be ok to set this as supportsEdgeId=true", found.property(strategy.getIdKey()).value());
                assertEquals("else", found.property("something").value());
            });

            try {
                v.addEdge("self", v, T.id, o, "something", "else", strategy.getIdKey(), "this should toss and exception as supportsVertexId=false");
                fail("An exception should be tossed here because supportsEdgeId=true");
            } catch (IllegalArgumentException iae) {
                assertNotNull(iae);
            }

            try {
                final Edge e = v.addEdge("self", v, T.id, o, "something", "else");
                e.property(strategy.getIdKey(), "this should toss and exception as supportsVertexId=false");
                fail("An exception should be tossed here because supportsEdgeId=true");
            } catch (IllegalArgumentException iae) {
                assertNotNull(iae);
            }
        }
    }

    public static class EdgeIdNotSupportedIdStrategyTest extends AbstractGremlinTest {
        public EdgeIdNotSupportedIdStrategyTest() {
            super(IdStrategy.build(idKey).supportsEdgeId(false).create());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        public void shouldInjectAnIdAndReturnBySpecifiedId() {
            final IdStrategy strategy = (IdStrategy) ((StrategyGraph) g).getStrategy();
            final Vertex v = g.addVertex(T.id, "test", "something", "else");
            final Edge e = v.addEdge("self", v, T.id, "edge-id", "try", "this");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertEquals("edge-id", e.id());
                assertFalse(e.property(strategy.getIdKey()).isPresent());
                assertEquals("this", e.property("try").value());

                final Edge found = g.E("edge-id").next();
                assertEquals("edge-id", found.id());
                assertFalse(found.property(strategy.getIdKey()).isPresent());
                assertEquals("this", found.property("try").value());
            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        public void shouldAllowDirectSettingOfIdField() {
            final IdStrategy strategy = (IdStrategy) ((StrategyGraph) g).getStrategy();
            final Vertex v = g.addVertex(T.id, "test", "something", "else");
            final Edge e = v.addEdge("self", v, T.id, "edge-id", "try", "this", strategy.getIdKey(), "should be ok to set this as supportsEdgeId=false");
            tryCommit(g, c -> {
                assertNotNull(e);
                assertEquals("edge-id", e.id());
                assertEquals("this", e.property("try").value());
                assertEquals("should be ok to set this as supportsEdgeId=false", e.property(strategy.getIdKey()).value());

                final Edge found = g.E("edge-id").next();
                assertEquals("edge-id", found.id());
                assertEquals("this", found.property("try").value());
                assertEquals("should be ok to set this as supportsEdgeId=false", found.property(strategy.getIdKey()).value());
            });

            try {
                g.addVertex(T.id, "test", "something", "else", strategy.getIdKey(), "this should toss and exception as supportsVertexId=true");
                fail("An exception should be tossed here because supportsVertexId=true");
            } catch (IllegalArgumentException iae) {
                assertNotNull(iae);
            }

            try {
                v.property(strategy.getIdKey(), "this should toss and exception as supportsVertexId=true");
                fail("An exception should be tossed here because supportsVertexId=true");
            } catch (IllegalArgumentException iae) {
                assertNotNull(iae);
            }
        }
    }
}
