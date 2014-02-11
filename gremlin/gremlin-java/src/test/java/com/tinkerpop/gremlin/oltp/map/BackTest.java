package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.oltp.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BackTest extends com.tinkerpop.gremlin.process.oltp.map.BackTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_asXhereX_out_backXhereX() {
        // super.g_v1_asXhereX_out_backXhereX(GremlinJ.of(g).v(1).as("here").out().back("here"));
    }

    @Test
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX() {
        // super.g_v4_out_asXhereX_hasXlang_javaX_backXhereX(GremlinJ.of(g).v(4).out().as("here").has("lang", "java").back("here"));
    }

    @Test
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX() {
        // super.g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(GremlinJ.of(g).v(4).out().as("here").has("lang", "java").back("here").<String>value("name"));
    }

    @Test
    public void g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX() {
        //  super.g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(GremlinJ.of(g).v(1).outE().as("here").inV().has("name", "vadas").back("here"));
    }

    @Test
    public void g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX() {
        //  super.g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(GremlinJ.of(g).v(1).outE("knows").has("weight", 1.0f).as("here").inV().has("name", "josh").back("here"));
    }

}