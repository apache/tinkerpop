package com.tinkerpop.gremlin.olap.filter;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.process.oltp.ComplianceTest;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterTest extends com.tinkerpop.gremlin.process.oltp.filter.FilterTest {

    Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_filterXfalseX() {
        // super.g_V_filterXfalseX(new GremlinResult<>(g, () -> GremlinJ.of().V().filter(v -> false)));
    }

    @Test
    public void g_V_filterXtrueX() {
        //  super.g_V_filterXtrueX(new GremlinResult<>(g, () -> GremlinJ.of().V().filter(v -> true)));
    }

    @Test
    public void g_V_filterXlang_eq_javaX() {
        //  super.g_V_filterXlang_eq_javaX(new GremlinResult<>(g, () -> GremlinJ.of().V().filter(v -> v.get().<String>getProperty("lang").orElse("none").equals("java"))));
    }

    @Test
    public void g_v1_out_filterXage_gt_30X() {
        //  super.g_v1_out_filterXage_gt_30X(new GremlinResult<>(g, () -> GremlinJ.of().v("1").out().filter(v -> v.get().<Integer>getProperty("age").orElse(0) > 30)));
    }

    @Test
    public void g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
        //  super.g_V_filterXname_startsWith_m_OR_name_startsWith_pX(new GremlinResult<>(g, () -> GremlinJ.of().V().filter(v -> {
        //      final String name = v.get().getValue("name");
        //      return name.startsWith("m") || name.startsWith("p");
        //  })));
    }
}