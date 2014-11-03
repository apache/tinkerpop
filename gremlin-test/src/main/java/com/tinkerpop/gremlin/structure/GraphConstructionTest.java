package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.util.GraphFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
        final Configuration c = graphProvider.newGraphConfiguration("temp1", this.getClass(), name.getMethodName());
        final Graph createdGraph = GraphFactory.open(c);

        assertNotNull(createdGraph);
        assertEquals(expectedGraph.getClass(), createdGraph.getClass());

        graphProvider.clear(createdGraph, c);
    }

    /**
     * Graphs should be empty on creation.
     */
    @Test
    public void shouldConstructAnEmptyGraph() {
        assertVertexEdgeCounts(0, 0).accept(g);
    }

    /**
     * A {@link Graph} should maintain the original {@code Configuration} object passed to it via {@link GraphFactory}.
     */
    @Test
    public void shouldMaintainOriginalConfigurationObjectGivenToFactory() {
        final Configuration originalConfig = graphProvider.newGraphConfiguration("temp2", this.getClass(), name.getMethodName());
        final Graph createdGraph = GraphFactory.open(originalConfig);

        final Configuration configInGraph = createdGraph.configuration();
        final AtomicInteger keyCount = new AtomicInteger(0);
        originalConfig.getKeys().forEachRemaining(k -> {
            assertTrue(configInGraph.containsKey(k));
            keyCount.incrementAndGet();
        });

        // need some keys in the originalConfig for this test to be meaningful
        assertTrue(keyCount.get() > 0);
        assertEquals(keyCount.get(), StreamFactory.stream(configInGraph.getKeys()).count());
    }
}
