package com.tinkerpop.gremlin.pipes.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RangeTest extends com.tinkerpop.gremlin.test.filter.RangeTest {

    Graph g = TinkerFactory.createClassic();

    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    public void test_g_v1_out_rangeX0_1X() {
        super.test_g_v1_out_rangeX0_1X(Gremlin.of(g).v(1).out().range(0, 1));
    }

    public void test_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV() {
        super.test_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(Gremlin.of(g).v(1).out("knows").outE("created").range(0, 0).inV());
    }

    public void test_g_v1_outXknowsX_outXcreatedX_rangeX0_0X() {
        super.test_g_v1_outXknowsX_outXcreatedX_rangeX0_0X(Gremlin.of(g).v(1).out("knows").out("created").range(0, 0));
    }

    public void test_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X() {
        super.test_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(Gremlin.of(g).v(1).out("created").in("created").range(1, 2));
    }

    public void test_g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV() {
        super.test_g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV(Gremlin.of(g).v(1).out("created").inE("created").range(1, 2).outV());
    }

}
