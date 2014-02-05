package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.GremlinJ;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalTest extends com.tinkerpop.gremlin.test.filter.IntervalTest {

    Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_outE_intervalXweight_0_06X_inV() {
        super.g_v1_outE_intervalXweight_0_06X_inV(GremlinJ.of(g).v(1).outE().interval("weight", 0.0f, 0.6f).inV());
    }
}