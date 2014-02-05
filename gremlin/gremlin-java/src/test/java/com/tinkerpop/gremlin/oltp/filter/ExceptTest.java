package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.GremlinJ;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExceptTest extends com.tinkerpop.gremlin.test.filter.ExceptTest {

    final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_out_exceptXg_v2X() {
        super.g_v1_out_exceptXg_v2X(GremlinJ.of(g).v(1).out().except(g.v(2).get()));
    }

    @Test
    public void g_v1_out_aggregateXxX_out_exceptXxX() {
        super.g_v1_out_aggregateXxX_out_exceptXxX(GremlinJ.of(g).v(1).out().aggregate("x").out().except("x"));
    }

    @Test
    public void g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX() {
        super.g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX(
                GremlinJ.of(g).v(1).out("created").in("created").except(g.v(1).get()).value("name"));
    }
}
