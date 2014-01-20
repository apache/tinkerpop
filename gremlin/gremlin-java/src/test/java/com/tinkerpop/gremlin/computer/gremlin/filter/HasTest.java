package com.tinkerpop.gremlin.computer.gremlin.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.T;
import com.tinkerpop.gremlin.computer.gremlin.GremlinResultIterable;
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
        super.g_V_hasXname_markoX(new GremlinResultIterable<Vertex>(g, () -> Gremlin.of().has("name", "marko")).iterator());
    }

    @Test
    public void g_V_hasXname_blahX() {
        super.g_V_hasXname_blahX(new GremlinResultIterable<Vertex>(g, () -> Gremlin.of().has("name", "blah")).iterator());
    }

    @Test
    public void g_V_hasXblahX() {
        super.g_V_hasXblahX(new GremlinResultIterable<Vertex>(g, () -> Gremlin.of().has("blah")).iterator());
    }

    @Test
    public void g_v1_out_hasXid_2X() {
        super.g_v1_out_hasXid_2X(new GremlinResultIterable<Vertex>(g, () -> Gremlin.of().has("id", "1").out().has("id", "2")).iterator());
    }

    @Test
    public void g_V_hasXage_gt_30X() {
        super.g_V_hasXage_gt_30X(new GremlinResultIterable<Vertex>(g, () -> Gremlin.of().has("age", T.gt, 30)).iterator());
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
