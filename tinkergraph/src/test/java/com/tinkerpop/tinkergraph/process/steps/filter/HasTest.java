package com.tinkerpop.tinkergraph.process.steps.filter;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.steps.ComplianceTest;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HasTest extends com.tinkerpop.gremlin.process.steps.filter.HasTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_hasXname_markoX() {
        super.g_V_hasXname_markoX(g.V().has("name", "marko"));
    }

    @Test
    public void g_V_hasXname_blahX() {
        super.g_V_hasXname_blahX(g.V().has("name", "blah"));
    }

    @Test
    public void g_V_hasXblahX() {
        super.g_V_hasXblahX(g.V().has("blah"));
    }

    @Test
    public void g_v1_out_hasXid_2X() {
        //  super.g_v1_out_hasXid_2X(GremlinJ.of(g).v(1).out().has("id", "2"));
    }

    @Test
    public void g_V_hasXage_gt_30X() {
        super.g_V_hasXage_gt_30X(g.V().has("age", T.gt, 30));
    }

    @Test
    public void g_E_hasXlabelXknowsX() {
        //super.g_E_hasXlabelXknowsX(g.E().has("label", "knows"));
    }

    @Test
    public void g_E_hasXlabelXknows_createdX() {
        //  super.g_E_hasXlabelXknows_createdX(GremlinJ.of(g).E().has("label", T.in, Arrays.asList("knows", "created")));
    }
}