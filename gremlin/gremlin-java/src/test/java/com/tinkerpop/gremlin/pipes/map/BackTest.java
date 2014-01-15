package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackTest extends com.tinkerpop.gremlin.test.map.BackTest {

    Graph g = TinkerFactory.createClassic();

    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    public void test_g_v1_asXhereX_out_backXhereX() {
        super.test_g_v1_asXhereX_out_backXhereX(Gremlin.of(g).v(1).as("here").out().back("here"));
    }

    public void test_g_v4_out_asXhereX_hasXlang_javaX_backXhereX() {
        super.test_g_v4_out_asXhereX_hasXlang_javaX_backXhereX(Gremlin.of(g).v(4).out().as("here").has("lang", "java").back("here"));
    }

    public void test_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX() {
        super.test_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(Gremlin.of(g).v(4).out().as("here").has("lang", "java").back("here").<String>value("name"));
    }

    public void test_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX() {
        super.test_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(Gremlin.of(g).v(1).outE().as("here").inV().has("name", "vadas").back("here"));
    }

}