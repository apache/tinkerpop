package com.tinkerpop.tinkergraph.process.oltp.map;

import com.tinkerpop.gremlin.process.oltp.ComplianceTest;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PathTest extends com.tinkerpop.gremlin.process.oltp.map.PathTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_propertyXnameX_path() {
        // super.g_v1_propertyXnameX_path(GremlinJ.of(g).v(1).value("name").path());
    }

    @Test
    public void g_v1_out_pathXage_nameX() {
        //  super.g_v1_out_pathXage_nameX(
        //          GremlinJ.of(g).v(1).out().path(v -> ((Vertex) v).getValue("age"), v -> ((Vertex) v).getValue("name")));

    }

    @Test
    public void g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX() {
//        super.g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX(
 //               g.V().as("x").out()
  //                      .jump("x", o -> o.getLoops() < 2)
   //                     .path(v -> v, v -> ((Vertex) v).getValue("name"), v -> ((Vertex) v).getValue("lang")));
    }
}