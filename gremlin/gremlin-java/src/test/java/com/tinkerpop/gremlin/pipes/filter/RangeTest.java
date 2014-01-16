package com.tinkerpop.gremlin.pipes.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RangeTest extends com.tinkerpop.gremlin.test.filter.RangeTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_out_rangeX0_1X() {
        super.g_v1_out_rangeX0_1X(Gremlin.of(g).v(1).out().range(0, 1));
    }

    @Test
    public void g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV() {
        super.g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(Gremlin.of(g).v(1).out("knows").outE("created").range(0, 0).inV());
    }

    @Test
    public void g_v1_outXknowsX_outXcreatedX_rangeX0_0X() {
        super.g_v1_outXknowsX_outXcreatedX_rangeX0_0X(Gremlin.of(g).v(1).out("knows").out("created").range(0, 0));
    }

    @Test
    public void g_v1_outXcreatedX_inXcreatedX_rangeX1_2X() {
        super.g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(Gremlin.of(g).v(1).out("created").in("created").range(1, 2));
    }

    @Test
    public void g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV() {
        super.g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV(Gremlin.of(g).v(1).out("created").inE("created").range(1, 2).outV());
    }

}
