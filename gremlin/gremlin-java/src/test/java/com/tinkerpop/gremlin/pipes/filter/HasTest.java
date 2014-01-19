package com.tinkerpop.gremlin.pipes.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.T;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HasTest extends com.tinkerpop.gremlin.test.filter.HasTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_hasXname_markoX() {
        super.g_V_hasXname_markoX(Gremlin.of(g, false).V().has("name", "marko"));   // TODO: Do this everywhere? More programmatic way to do this?
        super.g_V_hasXname_markoX(Gremlin.of(g).V().has("name", "marko"));
    }

    @Test
    public void g_V_hasXname_blahX() {
        super.g_V_hasXname_blahX(Gremlin.of(g).V().has("name", "blah"));
    }

    @Test
    public void g_V_hasXblahX() {
        super.g_V_hasXblahX(Gremlin.of(g).V().has("blah"));
    }

    @Test
    public void g_v1_out_hasXid_2X() {
        super.g_v1_out_hasXid_2X(Gremlin.of(g).v(1).out().has("id", "2"));
    }

    @Test
    public void g_V_hasXage_gt_30X() {
        super.g_V_hasXage_gt_30X(Gremlin.of(g).V().has("age", T.gt, 30));
    }

    @Test
    public void g_E_hasXlabelXknowsX() {
        super.g_E_hasXlabelXknowsX(Gremlin.of(g).E().has("label", T.eq, "knows"));
    }

    @Test
    public void g_E_hasXlabelXknows_createdX() {
        super.g_E_hasXlabelXknows_createdX(Gremlin.of(g).E().has("label", T.in, Arrays.asList("knows", "created")));
    }
}