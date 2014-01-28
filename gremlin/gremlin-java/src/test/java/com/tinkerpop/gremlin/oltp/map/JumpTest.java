package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpTest extends com.tinkerpop.gremlin.test.map.JumpTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX() {
        super.g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(Gremlin.of(g).v(1).as("x").out().jump("x", h -> h.getLoops() < 2).value("name"));
    }
}
