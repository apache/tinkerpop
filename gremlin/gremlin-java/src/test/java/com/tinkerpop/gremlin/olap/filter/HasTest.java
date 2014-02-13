package com.tinkerpop.gremlin.olap.filter;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.steps.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasTest extends com.tinkerpop.gremlin.process.steps.filter.HasTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_hasXname_markoX() {
        //super.g_V_hasXname_markoX(new TraversalResult<>(g, () -> GremlinJ.of().V().has("name", "marko")));
    }

    @Test
    public void g_V_hasXname_blahX() {
        // super.g_V_hasXname_blahX(new TraversalResult<>(g, () -> GremlinJ.of().V().has("name", "blah")));
    }

    @Test
    public void g_V_hasXblahX() {
        // super.g_V_hasXblahX(new TraversalResult<>(g, () -> GremlinJ.of().V().has("blah")));
    }

    @Test
    public void g_v1_out_hasXid_2X() {
        //  super.g_v1_out_hasXid_2X(new TraversalResult<>(g, () -> GremlinJ.of().v("1").out().has("id", "2")));
    }

    @Test
    public void g_V_hasXage_gt_30X() {
        //  super.g_V_hasXage_gt_30X(new TraversalResult<>(g, () -> GremlinJ.of().V().has("age", T.gt, 30)));
    }

    @Test
    public void g_E_hasXlabelXknowsX() {
        // super.g_E_hasXlabelXknowsX(new TraversalResult<>(g, () -> GremlinJ.of().E().has("label", T.eq, "knows")));
    }

    @Test
    public void g_E_hasXlabelXknows_createdX() {
        // super.g_E_hasXlabelXknows_createdX(new TraversalResult<>(g, () -> GremlinJ.of().E().has("label", T.in, Arrays.asList("knows", "created"))));
    }
}
