package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.oltp.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExceptTest extends com.tinkerpop.gremlin.process.oltp.filter.ExceptTest {

    final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_out_exceptXg_v2X() {
        // super.g_v1_out_exceptXg_v2X(GremlinJ.of(g).v(1).out().except(g.v(2).get()));
    }

    @Test
    public void g_v1_out_aggregateXxX_out_exceptXxX() {
        // super.g_v1_out_aggregateXxX_out_exceptXxX(GremlinJ.of(g).v(1).out().aggregate("x").out().except("x"));
    }

    @Test
    public void g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX() {
        //  super.g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX(
        //         GremlinJ.of(g).v(1).out("created").in("created").except(g.v(1).get()).value("name"));
    }
}
