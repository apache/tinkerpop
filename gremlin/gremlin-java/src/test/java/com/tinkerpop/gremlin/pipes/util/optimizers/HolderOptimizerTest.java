package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.Gremlin;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderOptimizerTest {

    @Test
    public void shouldTurnPathTrackingOn() {
        Gremlin gremlin = Gremlin.of(TinkerGraph.open()).V();
        //assertFalse(gremlin.getTrackPaths());
        new HolderOptimizer().optimize(gremlin);
        //assertFalse(gremlin.getTrackPaths());
        gremlin.path();
        new HolderOptimizer().optimize(gremlin);
        //assertTrue(gremlin.getTrackPaths());
    }
}
