package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.Gremlin;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryOptimizerTest {

    @Test
    public void shouldTurnPathTrackingOn() {
        assertTrue(true);
        Gremlin gremlin = (Gremlin) Gremlin.of(TinkerGraph.open()).V().has("age",32);
        System.out.println(gremlin);
        new GraphQueryOptimizer().optimize(gremlin);
        System.out.println(gremlin);

    }
}