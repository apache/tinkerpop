package com.tinkerpop.gremlin.oltp.sideffect;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.GremlinJ;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

import java.util.HashSet;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AggregateTest extends com.tinkerpop.gremlin.test.sideeffect.AggregateTest {

    Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_v1_aggregateXaX_outXcreatedX_inXcreatedX_exceptXaX() {
        super.g_v1_aggregateXaX_outXcreatedX_inXcreatedX_exceptXaX(
                GremlinJ.of(g).with("x", new HashSet<>())
                        .v(1).aggregate("x").out("created").in("created").except("x"));
    }

    @Test
    public void g_V_valueXnameX_aggregateXaX_iterate_getXaX() {
        super.g_V_valueXnameX_aggregateXaX_iterate_getXaX(GremlinJ.of(g).V().value("name").aggregate("x").iterate().memory().get("x"));
    }

    @Test
    public void g_V_aggregateXa_nameX_iterate_getXaX() {
        super.g_V_aggregateXa_nameX_iterate_getXaX(GremlinJ.of(g).V().aggregate("a", v -> v.getValue("name")).iterate().memory().get("a"));
    }
}