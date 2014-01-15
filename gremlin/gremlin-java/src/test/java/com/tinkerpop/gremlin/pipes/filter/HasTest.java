package com.tinkerpop.gremlin.pipes.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.T;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasTest extends com.tinkerpop.gremlin.test.filter.HasTest {

    Graph g = TinkerFactory.createClassic();

    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    public void test_g_V_hasXname_markoX() {
        super.test_g_V_hasXname_markoX(Gremlin.of(g).V().has("name", "marko"));
    }

    public void test_g_V_hasXname_blahX() {
        super.test_g_V_hasXname_blahX(Gremlin.of(g).V().has("name", "blah"));
    }

    public void test_g_V_hasXblahX() {
        super.test_g_V_hasXblahX(Gremlin.of(g).V().has("blah"));
    }

    public void test_g_v1_out_hasXid_2X() {
        super.test_g_v1_out_hasXid_2X(Gremlin.of(g).v(1).out().has("id", "2"));
    }

    public void test_g_V_hasXage_gt_30X() {
        super.test_g_V_hasXage_gt_30X(Gremlin.of(g).V().has("age", T.gt, 30));
    }

    public void test_g_E_hasXlabelXknowsX() {
        super.test_g_E_hasXlabelXknowsX(Gremlin.of(g).E().has("label", T.eq, "knows"));
    }

    public void test_g_E_hasXlabelXknows_createdX() {
        super.test_g_E_hasXlabelXknows_createdX(Gremlin.of(g).E().has("label", T.in, Arrays.asList("knows", "created")));
    }
}