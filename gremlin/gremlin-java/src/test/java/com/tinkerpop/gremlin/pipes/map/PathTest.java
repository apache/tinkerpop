package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.PathHolder;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;
import com.tinkerpop.gremlin.pipes.util.HolderIterator;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PathTest extends com.tinkerpop.gremlin.test.map.PathTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_propertyXnameX_path() {
        super.g_v1_propertyXnameX_path(Gremlin.of(g).v(1).value("name").path());
    }

    @Test
    public void g_v1_out_pathXage_nameX() {
        super.g_v1_out_pathXage_nameX(
                Gremlin.of(g).v(1).out().path(v -> ((Vertex) v).getValue("age"), v -> ((Vertex) v).getValue("name")));

    }

    @Test
    public void g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX() {
        super.g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX(
                Gremlin.of(g).V().as("x").out()
                        .jump("x", o -> o.getLoops() < 2)
                        .path(v -> v, v -> ((Vertex) v).getValue("name"), v -> ((Vertex) v).getValue("lang")));
    }
}