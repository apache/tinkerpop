package com.tinkerpop.gremlin.olap.filter;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.oltp.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalTest extends com.tinkerpop.gremlin.process.oltp.filter.IntervalTest {

    Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_outE_intervalXweight_0_06X_inV() {
        // super.g_v1_outE_intervalXweight_0_06X_inV(new GremlinResult<>(g, () -> GremlinJ.of().v(1).outE().interval("weight", 0.0f, 0.6f).inV()));
    }
}