package com.tinkerpop.blueprints;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GraphTest {
    @Test
    public void shouldOpenInMemoryGraphViaApacheConfig() {
        final Configuration conf = BlueprintsSuite.GraphManager.get().newGraphConfiguration();
        final Graph expectedGraph = BlueprintsSuite.GraphManager.get().newTestGraph();
        final Graph g = GraphFactory.open(conf);

        assertNotNull(g);
        assertEquals(expectedGraph.getClass(), g.getClass());
    }
}
