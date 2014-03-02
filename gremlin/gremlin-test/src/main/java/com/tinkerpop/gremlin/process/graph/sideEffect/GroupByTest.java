package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class GroupByTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_groupByXa_nameX();

    public abstract Map<String, List<String>> get_g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX();

    public abstract Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXa_lang_1_countX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_groupByXa_nameX() {
        final Traversal<Vertex, Map<String, List<Vertex>>> traversal = get_g_V_groupByXa_nameX();
        System.out.println("Testing: " + traversal);
        final Map<String, List<Vertex>> map = traversal.next();
        assertEquals(6, map.size());
        map.forEach((key, values) -> {
            assertEquals(1, values.size());
            if (key.equals("marko")) {
                assertEquals("1", values.get(0).getId());
            } else if (key.equals("vadas")) {
                assertEquals("2", values.get(0).getId());
            } else if (key.equals("lop")) {
                assertEquals("3", values.get(0).getId());
            } else if (key.equals("josh")) {
                assertEquals("4", values.get(0).getId());
            } else if (key.equals("ripple")) {
                assertEquals("5", values.get(0).getId());
            } else if (key.equals("peter")) {
                assertEquals("6", values.get(0).getId());
            } else {
                throw new IllegalStateException("This is not a possible return value");
            }
        });
    }

    @Ignore // TODO:
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX() {
        final Map<String, List<String>> map = get_g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX();
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(2, map.get("java").size());
        assertTrue(map.get("java").contains("ripple"));
        assertTrue(map.get("java").contains("lop"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXlangX_groupByXa_lang_1_countX() {
        final Traversal<Vertex, Map<String, Integer>> traversal = get_g_V_hasXlangX_groupByXa_lang_1_countX();
        System.out.println("Testing: " + traversal);
        final Map<String, Integer> map = traversal.next();
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(Integer.valueOf(2), map.get("java"));
    }

    public static class JavaGroupByTest extends GroupByTest {
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_groupByXa_nameX() {
            return (Traversal) g.V().groupBy(v -> v.getValue("name"));
        }

        public Map<String, List<String>> get_g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX() {
            return g.V().<Vertex>has("lang")
                    .groupBy("a",
                            v -> v.getValue("lang"),
                            v -> v.getValue("name")).iterate().memory().get("a");
        }

        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXa_lang_1_countX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .groupBy(v -> v.getValue("lang"),
                            v -> 1,
                            vv -> vv.size());
        }
    }

}
