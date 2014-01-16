package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BackTest extends com.tinkerpop.gremlin.test.map.BackTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_asXhereX_out_backXhereX() {
        super.g_v1_asXhereX_out_backXhereX(Gremlin.of(g).v(1).as("here").out().back("here"));
    }

    @Test
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX() {
        super.g_v4_out_asXhereX_hasXlang_javaX_backXhereX(Gremlin.of(g).v(4).out().as("here").has("lang", "java").back("here"));
    }

    @Test
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX() {
        super.g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(Gremlin.of(g).v(4).out().as("here").has("lang", "java").back("here").<String>value("name"));
    }

    @Test
    public void g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX() {
        super.g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(Gremlin.of(g).v(1).outE().as("here").inV().has("name", "vadas").back("here"));
    }

}