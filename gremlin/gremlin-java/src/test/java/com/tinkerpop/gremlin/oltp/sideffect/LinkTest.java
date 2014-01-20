package com.tinkerpop.gremlin.oltp.sideffect;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkTest extends com.tinkerpop.gremlin.test.sideeffect.LinkTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void test_g_v1_asXaX_outXcreatedX_inXcreatedX_linkBothXcocreator_aX() {
        super.test_g_v1_asXaX_outXcreatedX_inXcreatedX_linkBothXcocreator_aX(Gremlin.of(g).v(1).as("a").out("created").in("created").linkBoth("cocreator", "a"));
    }
}
