package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ListTest extends com.tinkerpop.gremlin.test.map.ListTest {

    Graph g = TinkerFactory.createModern();

    @Test
    public void testCompliance() {
        // ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_listXlocationsX_intervalXstartTime_2004_2006X() {
        super.g_v1_listXlocationsX_intervalXstartTime_2004_2006X(Gremlin.of(g).v(1).list("locations").interval("startTime", 2004, 2006));
    }

    @Test
    public void g_V_hasXlocationsX_listXlocationsX_hasXstartTime_2005X_value() {
        super.g_V_hasXlocationsX_listXlocationsX_hasXstartTime_2005X_value(Gremlin.of(g).V().has("locations").list("locations").has("startTime", 2005).value());
    }
}