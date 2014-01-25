package com.tinkerpop.gremlin.olap.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.T;
import com.tinkerpop.gremlin.olap.util.GremlinResult;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasTest extends com.tinkerpop.gremlin.test.filter.HasTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_hasXname_markoX() {
        super.g_V_hasXname_markoX(new GremlinResult<>(g, () -> Gremlin.of().V().has("name", "marko")));
    }

    @Test
    public void g_V_hasXname_blahX() {
        super.g_V_hasXname_blahX(new GremlinResult<>(g, () -> Gremlin.of().V().has("name", "blah")));
    }

    @Test
    public void g_V_hasXblahX() {
        super.g_V_hasXblahX(new GremlinResult<>(g, () -> Gremlin.of().V().has("blah")));
    }

    @Test
    public void g_v1_out_hasXid_2X() {
        super.g_v1_out_hasXid_2X(new GremlinResult<>(g, () -> Gremlin.of().v("1").out().has("id", "2")));
    }

    @Test
    public void g_V_hasXage_gt_30X() {
        super.g_V_hasXage_gt_30X(new GremlinResult<>(g, () -> Gremlin.of().V().has("age", T.gt, 30)));
    }

    @Test
    public void g_E_hasXlabelXknowsX() {
        super.g_E_hasXlabelXknowsX(new GremlinResult<>(g, () -> Gremlin.of().E().has("label", T.eq, "knows")));
    }

    @Test
    public void g_E_hasXlabelXknows_createdX() {
        super.g_E_hasXlabelXknows_createdX(new GremlinResult<>(g, () -> Gremlin.of().E().has("label", T.in, Arrays.asList("knows", "created"))));
    }
}
