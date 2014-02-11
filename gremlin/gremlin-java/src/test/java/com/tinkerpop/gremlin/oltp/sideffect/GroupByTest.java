package com.tinkerpop.gremlin.oltp.sideffect;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupByTest extends com.tinkerpop.gremlin.process.oltp.sideEffect.GroupByTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        // TODO: WHY?!?! ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_groupByXa_nameX() {
        // super.g_V_groupByXa_nameX(GremlinJ.of(g).V().groupBy(v -> v.getValue("name")));
    }

    @Test
    public void g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX() {
        //  super.g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX(
        //          GremlinJ.of(g).V().<Vertex>has("lang")
        //                  .groupBy("a",
        //                          v -> v.getValue("lang"),
        //                          v -> v.getValue("name")).iterate().memory().get("a"));
    }

    @Test
    public void g_V_hasXlangX_groupByXa_lang_1_countX() {
        // super.g_V_hasXlangX_groupByXa_lang_1_countX(
        //          GremlinJ.of(g).V().<Vertex>has("lang")
        //                  .groupBy(v -> v.getValue("lang"),
        //                         v -> 1,
        //                         vv -> vv.size()));
    }
}
