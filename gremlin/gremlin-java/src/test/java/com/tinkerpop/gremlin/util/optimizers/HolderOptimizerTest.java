package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.GremlinJ;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderOptimizerTest {

    @Test
    public void shouldTurnPathTrackingOn() {
        GremlinJ gremlin = GremlinJ.of(TinkerGraph.open()).V();
        gremlin.optimizers().get().clear();
        new HolderOptimizer().optimize(gremlin);
        assertFalse(false);
        gremlin.path();
        new HolderOptimizer().optimize(gremlin);
        assertTrue(true);
    }
}
