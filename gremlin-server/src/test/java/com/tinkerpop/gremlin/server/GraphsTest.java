package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphsTest {
    @Test(expected = UnsupportedOperationException.class)
    public void shouldReturnImmutableMapOfGraphs() {
        final Settings settings = Settings.read(GraphsTest.class.getResourceAsStream("gremlin-server-integration.yaml")).get();
        final Graphs graphs = new Graphs(settings);
        final Map<String, Graph> m = graphs.getGraphs();

        m.put("h", TinkerGraph.open());
    }

    @Test
    public void shouldReturnGraphs() {
        final Settings settings = Settings.read(GraphsTest.class.getResourceAsStream("gremlin-server-integration.yaml")).get();
        final Graphs graphs = new Graphs(settings);
        final Map<String, Graph> m = graphs.getGraphs();

        assertNotNull(m);
        assertTrue(m.containsKey("g"));
        assertTrue(m.get("g") instanceof TinkerGraph);
    }

    //TODO: test commit/rollbackall
}
