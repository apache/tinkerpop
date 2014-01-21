package com.tinkerpop.gremlin.oltp.sideffect;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

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
    public void g_V_outXcreatedX_name_groupCount() {
        super.g_V_outXcreatedX_name_groupCount(Gremlin.of(g).V().out("created").value("name").groupCount());
    }
}
