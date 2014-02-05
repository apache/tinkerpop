package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.GremlinJ;
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
        super.g_v1_out_rangeX0_1X(GremlinJ.of(g).v(1).out().range(0, 1));
    }

    @Test
    public void g_V_outX1X_rangeX0_2X() {
        super.g_V_outX1X_rangeX0_2X(GremlinJ.of(g).V().out(1).range(0, 2));
    }

    @Test
    public void g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV() {
        super.g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(GremlinJ.of(g).v(1).out("knows").outE("created").range(0, 0).inV());
    }

    @Test
    public void g_v1_outXknowsX_outXcreatedX_rangeX0_0X() {
        super.g_v1_outXknowsX_outXcreatedX_rangeX0_0X(GremlinJ.of(g).v(1).out("knows").out("created").range(0, 0));
    }

    @Test
    public void g_v1_outXcreatedX_inXcreatedX_rangeX1_2X() {
        super.g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(GremlinJ.of(g).v(1).out("created").in("created").range(1, 2));
    }

    @Test
    public void g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV() {
        super.g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV(GremlinJ.of(g).v(1).out("created").inE("created").range(1, 2).outV());
    }

}
