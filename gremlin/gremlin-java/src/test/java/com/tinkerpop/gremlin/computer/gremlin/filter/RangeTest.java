package com.tinkerpop.gremlin.computer.gremlin.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.computer.gremlin.GremlinResult;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RangeTest extends com.tinkerpop.gremlin.test.filter.RangeTest {

    private final Graph g = TinkerFactory.createClassic();

    /*@Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_out_rangeX0_1X() {
        super.g_v1_out_rangeX0_1X(new GremlinResult<>(g, () -> Gremlin.of().v(1).out().range(0, 1)));
    }

    @Test
    public void g_V_outX1X_rangeX0_2X() {
        super.g_V_outX1X_rangeX0_2X(new GremlinResult<>(g, () -> Gremlin.of().V().out(1).range(0, 2)));
    }

    @Test
    public void g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV() {
        super.g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(new GremlinResult<>(g, () -> Gremlin.of(g).v(1).out("knows").outE("created").range(0, 0).inV()));
    }

    @Test
    public void g_v1_outXknowsX_outXcreatedX_rangeX0_0X() {
        super.g_v1_outXknowsX_outXcreatedX_rangeX0_0X(new GremlinResult<>(g, () -> Gremlin.of(g).v(1).out("knows").out("created").range(0, 0)));
    }

    @Test
    public void g_v1_outXcreatedX_inXcreatedX_rangeX1_2X() {
        super.g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(new GremlinResult<>(g, () -> Gremlin.of(g).v(1).out("created").in("created").range(1, 2)));
    }

    @Test
    public void g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV() {
        super.g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV(new GremlinResult<>(g, () -> Gremlin.of(g).v(1).out("created").inE("created").range(1, 2).outV()));
    }*/

}
