package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.FeatureRequirement;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.Optional;

import static com.tinkerpop.blueprints.Graph.Features.GraphFeatures.FEATURE_STRATEGY;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategyTest extends AbstractBlueprintsTest {
    public ReadOnlyGraphStrategyTest() {
        super(Optional.of(new ReadOnlyGraphStrategy()));
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_STRATEGY)
    public void shouldNotAllowRemoveVertex() {
        final Vertex v = g.addVertex();
        v.remove();
    }
}
