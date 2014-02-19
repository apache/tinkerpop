package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Optional;

import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_STRATEGY;
import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_STRING_VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IdGraphStrategyTest extends AbstractGremlinTest {
    private static final String idKey = "myId";

    public IdGraphStrategyTest() {
        super(Optional.of(new IdGraphStrategy.Builder(idKey).build()));
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

            assertEquals("test", v.getProperty(Property.Key.hidden(idKey)).get());
            assertEquals("else", v.getProperty("something").get());

            final Vertex found = g.v("test");
            assertEquals(v, found);
            if (g.getFeatures().vertex().supportsUserSuppliedIds())
                assertEquals("test", found.getId());

            assertEquals("test", found.getProperty(Property.Key.hidden(idKey)).get());
            assertEquals("else", found.getProperty("something").get());

        });
    }

    // todo: test g.e()
}
