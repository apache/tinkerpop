package com.tinkerpop.tinkergraph.process.steps.map;

import com.tinkerpop.gremlin.process.steps.ComplianceTest;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderTest extends com.tinkerpop.gremlin.process.steps.map.OrderTest {

    Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_name_order() {
        super.g_V_name_order(g.V().<String>value("name").order());
    }

    @Test
    public void g_V_name_orderXabX() {
        super.g_V_name_orderXabX(g.V().<String>value("name").order((a, b) -> b.get().compareTo(a.get())));
    }

    @Test
    public void g_V_orderXa_nameXb_nameX_name() {
        super.g_V_orderXa_nameXb_nameX_name(g.V().order((a, b) -> a.get().<String>getValue("name").compareTo(b.get().getValue("name"))).value("name"));
    }
}