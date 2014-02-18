package com.tinkerpop.gremlin.process.olap.steps.map;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.steps.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackTest extends com.tinkerpop.gremlin.process.steps.map.BackTest {

    @Test
    public void g_v1_asXhereX_out_backXhereX() {
        // super.g_v1_asXhereX_out_backXhereX(new TraversalResult<>(g, () -> GremlinJ.of().v("1").as("here").out().back("here")));
    }

    @Test
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX() {
        //  super.g_v4_out_asXhereX_hasXlang_javaX_backXhereX(new TraversalResult<>(g, () -> GremlinJ.of().v("4").out().as("here").has("lang", "java").back("here")));
    }

    @Test
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX() {
        //  super.g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(new TraversalResult<>(g, () -> GremlinJ.of().v("4").out().as("here").has("lang", "java").back("here").<String>value("name")));
    }

    @Test
    public void g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX() {
        //  super.g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(new TraversalResult<>(g, () -> GremlinJ.of().v("1").outE().as("here").inV().has("name", "vadas").back("here")));
    }

    @Test
    public void g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX() {
        //  super.g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(new TraversalResult<>(g, () -> GremlinJ.of().v("1").outE("knows").has("weight", 1.0f).as("here").inV().has("name", "josh").back("here")));
    }

}