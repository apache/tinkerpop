package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
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
        public void shouldInjectAnIdAndReturnBySpecifiedId() {
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
        public void shouldCreateAnIdAndReturnByCreatedId() {
            final Vertex v = g.addVertex("something", "else");

            tryCommit(g, c -> {
                assertNotNull(v);
                assertNotNull(UUID.fromString(v.getProperty(Property.Key.hidden(idKey)).get().toString()));
                assertEquals("else", v.getProperty("something").get());

                final Object suppliedId = v.getId();
                final Vertex found = g.v(suppliedId);
                assertEquals(v, found);
                assertNotNull(UUID.fromString(found.getProperty(Property.Key.hidden(idKey)).get().toString()));
                assertEquals("else", found.getProperty("something").get());

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

    // todo: test edge creation still
}
