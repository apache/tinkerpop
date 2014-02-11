package com.tinkerpop.gremlin.olap.sideeffect;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.process.oltp.ComplianceTest;
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

    }

    @Test
    public void g_V_outXcreatedX_name_groupCount() {
        //super.g_V_outXcreatedX_name_groupCount(new GremlinResult<>(g, () -> Gremlin.of().V().out("created").value("name").groupCount());
    }

    @Test
    public void g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX() {

    }
}
