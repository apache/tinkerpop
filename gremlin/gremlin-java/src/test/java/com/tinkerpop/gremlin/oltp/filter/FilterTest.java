package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.GremlinJ;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterTest extends com.tinkerpop.gremlin.test.filter.FilterTest {

    Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_filterXfalseX() {
        super.g_V_filterXfalseX(GremlinJ.of(g).V().filter(v -> false));
    }

    @Test
    public void g_V_filterXtrueX() {
        super.g_V_filterXtrueX(GremlinJ.of(g).V().filter(v -> true));
    }

    @Test
    public void g_V_filterXlang_eq_javaX() {
        super.g_V_filterXlang_eq_javaX(GremlinJ.of(g).V().filter(v -> v.get().<String>getProperty("lang").orElse("none").equals("java")));
    }

    @Test
    public void g_v1_out_filterXage_gt_30X() {
        super.g_v1_out_filterXage_gt_30X(GremlinJ.of(g).v(1).out().filter(v -> v.get().<Integer>getProperty("age").orElse(0) > 30));
    }

    @Test
    public void g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
        super.g_V_filterXname_startsWith_m_OR_name_startsWith_pX(GremlinJ.of(g).V().filter(v -> {
            final String name = v.get().getValue("name");
            return name.startsWith("m") || name.startsWith("p");
        }));
    }
}