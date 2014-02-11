package com.tinkerpop.gremlin.oltp.sideffect;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.oltp.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountTest extends com.tinkerpop.gremlin.process.oltp.sideEffect.GroupCountTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_outXcreatedX_groupCountXnameX() {
        //  super.g_V_outXcreatedX_groupCountXnameX(GremlinJ.of(g).V().out("created").groupCount(v -> v.getValue("name")));
    }

    @Test
    public void g_V_outXcreatedX_name_groupCount() {
        // super.g_V_outXcreatedX_name_groupCount(GremlinJ.of(g).V().out("created").value("name").groupCount());
    }

    @Test
    public void g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX() {
        //  super.g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX(
        //          GremlinJ.of(g).V().as("x").out()
        //                  .groupCount("a", v -> v.getValue("name"))
        //                  .jump("x", h -> h.getLoops() < 2).iterate().memory().get("a"));
    }
}
