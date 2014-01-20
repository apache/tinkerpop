package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.util.optimizers.HolderOptimizer;
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
        gremlin.getOptimizers().clear();
        new HolderOptimizer().optimize(gremlin);
        assertFalse(false);
        gremlin.path();
        new HolderOptimizer().optimize(gremlin);
        assertTrue(true);
    }
}
