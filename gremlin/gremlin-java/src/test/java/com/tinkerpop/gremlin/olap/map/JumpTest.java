package com.tinkerpop.gremlin.olap.map;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.steps.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpTest extends com.tinkerpop.gremlin.process.steps.map.JumpTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX() {
        // super.g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(new GremlinResult<>(g, () -> GremlinJ.of().v("1").as("x").out().jump("x", h -> h.getLoops() < 2).value("name")));
    }
}
