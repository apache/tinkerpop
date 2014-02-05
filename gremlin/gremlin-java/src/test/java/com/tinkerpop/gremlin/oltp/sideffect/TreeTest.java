package com.tinkerpop.gremlin.oltp.sideffect;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.GremlinJ;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TreeTest extends com.tinkerpop.gremlin.test.sideeffect.TreeTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_out_out_treeXnameX() {
        super.g_v1_out_out_treeXnameX(GremlinJ.of(g).v(1).out().out().tree(v -> ((Vertex) v).getValue("name")));
    }
}