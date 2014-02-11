package com.tinkerpop.tinkergraph.process.oltp.map;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.oltp.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpTest extends com.tinkerpop.gremlin.process.oltp.map.JumpTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX() {
        //  super.g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(GremlinJ.of(g).v(1).as("x").out().jump("x", h -> h.getLoops() < 2).value("name"));
    }
}
