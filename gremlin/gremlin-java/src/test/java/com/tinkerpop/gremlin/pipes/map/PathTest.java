package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTest extends com.tinkerpop.gremlin.test.map.PathTest {

    final Graph g = TinkerFactory.createClassic();

    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    public void test_g_v1_propertyXnameX_path() {
        super.test_g_v1_propertyXnameX_path(Gremlin.of(g).v(1).value("name").path());
    }

    public void test_g_v1_out_pathXage__nameX() {
        super.test_g_v1_out_pathXage__nameX(
                Gremlin.of(g).v(1).out().path(v -> ((Vertex) v).getValue("age"), v -> ((Vertex) v).getValue("name")));

    }

    public void test_g_V_out_loopX1__loops_lt_3X_pathXit__name__langX() {
        super.test_g_V_out_loopX1__loops_lt_3X_pathXit__name__langX(
                Gremlin.of(g).V().as("x").out()
                        .loop("x", o -> o.getLoops() < 2)
                        .path(v -> v, v -> ((Vertex) v).getValue("name"), v -> ((Vertex) v).getValue("lang")));
    }
}