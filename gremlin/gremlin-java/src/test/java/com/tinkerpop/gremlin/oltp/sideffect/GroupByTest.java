package com.tinkerpop.gremlin.oltp.sideffect;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.Gremlin;
import org.junit.Test;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupByTest extends com.tinkerpop.gremlin.test.sideeffect.GroupByTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        // TODO: WHY?!?! ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_groupByXa_nameX() {
        super.g_V_groupByXa_nameX((Map) Gremlin.of(g).V().groupBy(v -> v.getValue("name")));
    }

    @Test
    public void g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX() {
        super.g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX((Map)
                Gremlin.of(g).V().has("lang")
                        .groupBy("a",
                                v -> v.getValue("lang"),
                                v -> v.getValue("name")).iterate().get("a"));
    }

    @Test
    public void g_V_hasXlangX_groupByXa_lang_1_countX() {
        super.g_V_hasXlangX_groupByXa_lang_1_countX((Map)
                Gremlin.of(g).V().has("lang")
                        .groupBy(v -> v.getValue("lang"),
                                v -> 1,
                                vv -> vv.size()));
    }
}
