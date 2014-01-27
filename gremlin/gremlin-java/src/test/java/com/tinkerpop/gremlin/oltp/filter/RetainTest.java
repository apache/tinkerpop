package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RetainTest extends com.tinkerpop.gremlin.test.filter.RetainTest {

    final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_out_retainXg_v2X() {
        super.g_v1_out_retainXg_v2X(Gremlin.of(g).v(1).out().retain(g.v(2).get()));
    }

    @Test
    public void g_v1_out_aggregateXxX_out_retainXxX() {
        super.g_v1_out_aggregateXxX_out_retainXxX(Gremlin.of(g).v(1).out().aggregate("x").out().retain("x"));
    }
}
