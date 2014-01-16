package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.strategy.GraphStrategy;
import com.tinkerpop.blueprints.strategy.PartitionGraphStrategy;
import com.tinkerpop.blueprints.util.GraphFactory;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Optional;

import static com.tinkerpop.blueprints.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that support the creation of {@link Graph} instances which occurs via {@link GraphFactory}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphConstructionTest extends AbstractBlueprintsTest{
    /**
     * All Blueprints implementations should be constructable through {@link GraphFactory}.
     */
    @Test
    public void shouldOpenGraphThroughGraphFactoryViaApacheConfig() {
        final Graph expectedGraph = g;
        final Graph createdGraph = GraphFactory.open(config, Optional.<GraphStrategy>empty());

        assertNotNull(createdGraph);
        assertEquals(expectedGraph.getClass(), createdGraph.getClass());
    }

    /**
     * If given a non-empty {@link GraphStrategy} a graph that does not support
     * {@link Graph.Features.GraphFeatures#FEATURE_STRATEGY} should throw
     * {@link com.tinkerpop.blueprints.Graph.Exceptions#graphStrategyNotSupported()}.
     */
    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
    public void shouldThrowUnsupportedIfStrategyIsNonEmptyAndStrategyFeatureDisabled() {
        try {
            GraphFactory.open(config, Optional.<GraphStrategy>of(new PartitionGraphStrategy("k","v")));
            fail("Strategy feature is not supported but accepts a GraphStrategy instance on construction.");
        } catch (UnsupportedOperationException ex) {
            assertEquals(Graph.Exceptions.graphStrategyNotSupported().getMessage(), ex.getMessage());
        }

    }

    /**
     * Blueprints implementations should have private constructor as all graphs.  They should be only instantiated
     * through the GraphFactory or the static open() method on the Graph implementation itself.
     */
    @Test
    public void shouldHavePrivateConstructor() {
        assertTrue(Arrays.asList(g.getClass().getConstructors()).stream().allMatch(c -> {
            final int modifier = c.getModifiers();
            return Modifier.isPrivate(modifier) || Modifier.isPrivate(modifier);
        }));
    }

    /**
     * Graphs should be empty on creation.
     */
    @Test
    public void shouldConstructAnEmptyGraph() {
        BlueprintsStandardSuite.assertVertexEdgeCounts(0, 0).accept(g);
    }
}
