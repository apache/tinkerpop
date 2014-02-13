package com.tinkerpop.tinkergraph.process.steps.sideEffect;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.steps.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkTest extends com.tinkerpop.gremlin.process.steps.sideEffect.LinkTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void test_g_v1_asXaX_outXcreatedX_inXcreatedX_linkBothXcocreator_aX() {
        // super.test_g_v1_asXaX_outXcreatedX_inXcreatedX_linkBothXcocreator_aX(GremlinJ.of(g).v(1).as("a").out("created").in("created").linkBoth("cocreator", "a"));
    }
}
