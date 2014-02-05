package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.blueprints.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.strategy.IdGraphStrategy;
import org.junit.Test;

import java.util.Optional;

import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_STRATEGY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IdGraphStrategyTest extends AbstractBlueprintsTest {
    private static final String idKey = "myId";

    public IdGraphStrategyTest() {
        super(Optional.of(new IdGraphStrategy(idKey)));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    public void shouldInjectAnIdAndReturnBySpecifiedId() {
        final Vertex v = g.addVertex(Element.ID, "test", "something", "else");

        assertNotNull(v);
        assertEquals("test", v.getId());
        assertEquals("test", v.getProperty(Property.Key.hidden(idKey)).get());
        assertEquals("else", v.getProperty("something").get());

        final Vertex found = g.query().ids("test").vertices().iterator().next();
        assertEquals(v, found);
    }
}
