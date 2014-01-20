package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalTest extends com.tinkerpop.gremlin.test.map.TraversalTest {

    final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V() {
        super.g_V(Gremlin.of(g).V());
    }

    @Test
    public void g_v1_out() {
        super.g_v1_out(Gremlin.of(g).v(1).out());
    }

    @Test
    public void g_v2_in() {
        super.g_v2_in(Gremlin.of(g).v(2).in());
    }

    @Test
    public void g_v4_both() {
        super.g_v4_both(Gremlin.of(g).v(4).both());
    }

    @Test
    public void g_v1_outX1_knowsX_name() {
        super.g_v1_outX1_knowsX_name(Gremlin.of(g).v(1).out(1, "knows").value("name"));
    }

    @Test
    public void g_V_bothX1_createdX_name() {
        super.g_V_bothX1_createdX_name(Gremlin.of(g).V().both(1, "created").value("name"));
    }

    @Test
    public void g_E() {
        super.g_E(Gremlin.of(g).E());
    }

    @Test
    public void g_v1_outE() {
        super.g_v1_outE(Gremlin.of(g).v(1).outE());
    }

    @Test
    public void g_v2_inE() {
        super.g_v2_inE(Gremlin.of(g).v(2).inE());
    }

    @Test
    public void g_v4_bothE() {
        super.g_v4_bothE(Gremlin.of(g).v(4).bothE());
    }

    @Test
    public void g_v4_bothEX1_createdX() {
        super.g_v4_bothEX1_createdX(Gremlin.of(g).v(4).bothE(1, "created"));
    }

    @Test
    public void g_V_inEX2_knowsX_outV_name() {
        super.g_V_inEX2_knowsX_outV_name(Gremlin.of(g).V().inE(2, "knows").outV().value("name"));
    }

    @Test
    public void g_v1_outE_inV() {
        super.g_v1_outE_inV(Gremlin.of(g).v(1).outE().inV());
    }

    @Test
    public void g_v2_inE_outV() {
        super.g_v2_inE_outV(Gremlin.of(g).v(2).inE().outV());
    }

    @Test
    public void g_v1_outXknowsX() {
        super.g_v1_outXknowsX(Gremlin.of(g).v(1).out("knows"));
    }

    @Test
    public void g_v1_outXknows_createdX() {
        super.g_v1_outXknows_createdX(Gremlin.of(g).v(1).out("knows", "created"));
    }

    @Test
    public void g_v1_outEXknowsX_inV() {
        super.g_v1_outEXknowsX_inV(Gremlin.of(g).v(1).outE("knows").inV());
    }

    @Test
    public void g_v1_outEXknows_createdX_inV() {
        super.g_v1_outEXknows_createdX_inV(Gremlin.of(g).v(1).outE("knows", "created").inV());
    }

    @Test
    public void g_V_out_out() {
        super.g_V_out_out(Gremlin.of(g).V().out().out());
    }

    @Test
    public void g_v1_out_out_out() {
        super.g_v1_out_out_out(Gremlin.of(g).v(1).out().out().out());
    }

    @Test
    public void g_v1_out_propertyXnameX() {
        super.g_v1_out_propertyXnameX(Gremlin.of(g).v(1).out().value("name"));
    }
}