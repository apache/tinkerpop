package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.GremlinJ;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderTest extends com.tinkerpop.gremlin.test.map.OrderTest {

    Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_name_order() {
        super.g_V_name_order(GremlinJ.of(g).V().<String>value("name").order());
    }

    @Test
    public void g_V_name_orderXabX() {
        super.g_V_name_orderXabX(GremlinJ.of(g).V().<String>value("name").order((a, b) -> b.get().compareTo(a.get())));
    }

    @Test
    public void g_V_orderXa_nameXb_nameX_name() {
        super.g_V_orderXa_nameXb_nameX_name(GremlinJ.of(g).V().order((a, b) -> a.get().<String>getValue("name").compareTo(b.get().getValue("name"))).value("name"));
    }
}