package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.GremlinJ;
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
        super.g_V(GremlinJ.of(g).V());
    }

    @Test
    public void g_v1_out() {
        super.g_v1_out(GremlinJ.of(g).v(1).out());
    }

    @Test
    public void g_v2_in() {
        super.g_v2_in(GremlinJ.of(g).v(2).in());
    }

    @Test
    public void g_v4_both() {
        super.g_v4_both(GremlinJ.of(g).v(4).both());
    }

    @Test
    public void g_v1_outX1_knowsX_name() {
        super.g_v1_outX1_knowsX_name(GremlinJ.of(g).v(1).out(1, "knows").value("name"));
    }

    @Test
    public void g_V_bothX1_createdX_name() {
        super.g_V_bothX1_createdX_name(GremlinJ.of(g).V().both(1, "created").value("name"));
    }

    @Test
    public void g_E() {
        super.g_E(GremlinJ.of(g).E());
    }

    @Test
    public void g_v1_outE() {
        super.g_v1_outE(GremlinJ.of(g).v(1).outE());
    }

    @Test
    public void g_v2_inE() {
        super.g_v2_inE(GremlinJ.of(g).v(2).inE());
    }

    @Test
    public void g_v4_bothE() {
        super.g_v4_bothE(GremlinJ.of(g).v(4).bothE());
    }

    @Test
    public void g_v4_bothEX1_createdX() {
        super.g_v4_bothEX1_createdX(GremlinJ.of(g).v(4).bothE(1, "created"));
    }

    @Test
    public void g_V_inEX2_knowsX_outV_name() {
        super.g_V_inEX2_knowsX_outV_name(GremlinJ.of(g).V().inE(2, "knows").outV().value("name"));
    }

    @Test
    public void g_v1_outE_inV() {
        super.g_v1_outE_inV(GremlinJ.of(g).v(1).outE().inV());
    }

    @Test
    public void g_v2_inE_outV() {
        super.g_v2_inE_outV(GremlinJ.of(g).v(2).inE().outV());
    }

    @Test
    public void g_V_outE_hasXweight_1X_outV() {
        super.g_V_outE_hasXweight_1X_outV(GremlinJ.of(g).V().outE().has("weight", 1.0f).outV());
    }

    @Test
    public void g_V_out_outE_inV_inE_inV_both_name() {
        super.g_V_out_outE_inV_inE_inV_both_name(GremlinJ.of(g).V().out().outE().inV().inE().inV().both().value("name"));
    }

    @Test
    public void g_v1_outEXknowsX_bothV_name() {
        super.g_v1_outEXknowsX_bothV_name(GremlinJ.of(g).v(1).outE("knows").bothV().value("name"));
    }

    @Test
    public void g_v1_outXknowsX() {
        super.g_v1_outXknowsX(GremlinJ.of(g).v(1).out("knows"));
    }

    @Test
    public void g_v1_outXknows_createdX() {
        super.g_v1_outXknows_createdX(GremlinJ.of(g).v(1).out("knows", "created"));
    }

    @Test
    public void g_v1_outEXknowsX_inV() {
        super.g_v1_outEXknowsX_inV(GremlinJ.of(g).v(1).outE("knows").inV());
    }

    @Test
    public void g_v1_outEXknows_createdX_inV() {
        super.g_v1_outEXknows_createdX_inV(GremlinJ.of(g).v(1).outE("knows", "created").inV());
    }

    @Test
    public void g_V_out_out() {
        super.g_V_out_out(GremlinJ.of(g).V().out().out());
    }

    @Test
    public void g_v1_out_out_out() {
        super.g_v1_out_out_out(GremlinJ.of(g).v(1).out().out().out());
    }

    @Test
    public void g_v1_out_propertyXnameX() {
        super.g_v1_out_propertyXnameX(GremlinJ.of(g).v(1).out().value("name"));
    }
}