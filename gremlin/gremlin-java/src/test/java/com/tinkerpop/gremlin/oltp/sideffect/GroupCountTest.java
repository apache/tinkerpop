package com.tinkerpop.gremlin.oltp.sideffect;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountTest extends com.tinkerpop.gremlin.test.sideeffect.GroupCountTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_outXcreatedX_groupCountXnameX() {
        super.g_V_outXcreatedX_groupCountXnameX(Gremlin.of(g).V().out("created").groupCount(v -> v.getValue("name")));
    }

    @Test
    public void g_V_outXcreatedX_name_groupCount() {
        super.g_V_outXcreatedX_name_groupCount(Gremlin.of(g).V().out("created").value("name").groupCount());
    }

    @Test
    public void g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX() {
        super.g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX(
                (Map) Gremlin.of(g).V().as("x").out()
                        .groupCount("a", v -> v.getValue("name"))
                        .jump("x", h -> h.getLoops() < 2).iterate().get("a"));
    }
}
