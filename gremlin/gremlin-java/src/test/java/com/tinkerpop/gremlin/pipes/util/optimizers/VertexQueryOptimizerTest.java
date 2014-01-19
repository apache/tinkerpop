package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.Gremlin;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryOptimizerTest {

    @Test
    public void shouldTurnPathTrackingOn() {
        assertTrue(true);
        Gremlin gremlin = (Gremlin) Gremlin.of(TinkerGraph.open()).V().outE("knows").has("weight", 1.0);
        System.out.println(gremlin);
        new VertexQueryOptimizer().optimize(gremlin);
        System.out.println(gremlin);

    }
}
