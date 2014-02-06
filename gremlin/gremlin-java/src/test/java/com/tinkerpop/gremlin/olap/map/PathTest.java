package com.tinkerpop.gremlin.olap.map;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.test.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTest extends com.tinkerpop.gremlin.test.map.PathTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_propertyXnameX_path() {
        // super.g_v1_propertyXnameX_path(new GremlinResult<>(g, () -> GremlinJ.of().v("1").value("name").path()));
    }

    @Test
    public void g_v1_out_pathXage_nameX() {
        //super.g_v1_out_pathXage_nameX(
        //        new GremlinResult<>(g, () -> Gremlin.of().v("1").out().path(v -> ((Vertex) v).getValue("age"), v -> ((Vertex) v).getValue("name"))));

    }

    @Test
    public void g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX() {
        // super.g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX(
        //           GremlinJ.of(g).V().as("x").out()
        //                  .jump("x", o -> o.getLoops() < 2)
        //                 .path(v -> v, v -> ((Vertex) v).getValue("name"), v -> ((Vertex) v).getValue("lang")));
    }
}