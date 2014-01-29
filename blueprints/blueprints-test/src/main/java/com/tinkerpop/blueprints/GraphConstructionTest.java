package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.strategy.GraphStrategy;
import com.tinkerpop.blueprints.util.GraphFactory;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
