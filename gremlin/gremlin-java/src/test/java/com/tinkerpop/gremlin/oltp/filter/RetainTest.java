package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.oltp.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RetainTest extends com.tinkerpop.gremlin.process.oltp.filter.RetainTest {

    final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_out_retainXg_v2X() {
        // super.g_v1_out_retainXg_v2X(GremlinJ.of(g).v(1).out().retain(g.v(2).get()));
    }

    @Test
    public void g_v1_out_aggregateXxX_out_retainXxX() {
        //  super.g_v1_out_aggregateXxX_out_retainXxX(GremlinJ.of(g).v(1).out().aggregate("x").out().retain("x"));
    }
}
