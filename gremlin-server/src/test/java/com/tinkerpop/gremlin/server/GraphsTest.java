package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphsTest {
    @Test
    @Ignore("Does not work on marko's local machine for some reason -- good for stephen and travis-ci")
    public void shouldReturnGraphs() {
        final Settings settings = Settings.read(GraphsTest.class.getResourceAsStream("gremlin-server-integration.yaml"));
        final Graphs graphs = new Graphs(settings);
        final Map<String, Graph> m = graphs.getGraphs();

        assertNotNull(m);
        assertTrue(m.containsKey("g"));
        assertTrue(m.get("g") instanceof TinkerGraph);
    }
}
