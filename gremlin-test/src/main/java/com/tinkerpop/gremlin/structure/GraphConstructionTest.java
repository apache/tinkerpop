package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that support the creation of {@link com.tinkerpop.gremlin.structure.Graph} instances which occurs via
 * {@link com.tinkerpop.gremlin.structure.util.GraphFactory}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphConstructionTest extends AbstractGremlinTest {
    /**
     * All Gremlin Structure implementations should be constructable through
     * {@link com.tinkerpop.gremlin.structure.util.GraphFactory}.
     */
    @Test
    public void shouldOpenGraphThroughGraphFactoryViaApacheConfig() throws Exception {
        final Graph expectedGraph = g;
        final Configuration c = graphProvider.newGraphConfiguration("temp", this.getClass(), name.getMethodName());
        final Graph createdGraph = GraphFactory.open(c);

        assertNotNull(createdGraph);
        assertEquals(expectedGraph.getClass(), createdGraph.getClass());

        graphProvider.clear(g, c);
    }

    /**
     * Gremlin Structure  implementations should have private constructor as all graphs.  They should be only
     * instantiated through the {@link com.tinkerpop.gremlin.structure.util.GraphFactory} or the static
     * {@link com.tinkerpop.gremlin.structure.util.GraphFactory#open(org.apache.commons.configuration.Configuration)}
     * method on the Graph implementation itself.
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
        assertVertexEdgeCounts(0, 0).accept(g);
    }
}
