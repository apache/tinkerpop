package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class GroupByTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_groupByXnameX();

    public abstract Traversal<Vertex, Map<String, List<String>>> get_g_V_hasXlangX_groupByXa_lang_nameX_out_capXaX();

    public abstract Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_groupByXa_nameX() {
        final Traversal<Vertex, Map<String, List<Vertex>>> traversal = get_g_V_groupByXnameX();
        System.out.println("Testing: " + traversal);
        final Map<String, List<Vertex>> map = traversal.next();
        assertEquals(6, map.size());
        map.forEach((key, values) -> {
            assertEquals(1, values.size());
            assertEquals(convertToVertexId(key), values.get(0).id());
        });
        assertFalse(traversal.hasNext());
    }

    @LoadGraphWith(CLASSIC)
    public void g_V_hasXlangX_groupByXa_lang_nameX_out_capXaX() {
        final Traversal<Vertex, Map<String, List<String>>> traversal = get_g_V_hasXlangX_groupByXa_lang_nameX_out_capXaX();
        System.out.println("Testing: " + traversal);
        final Map<String, List<String>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(2, map.get("java").size());
        assertTrue(map.get("java").contains("ripple"));
        assertTrue(map.get("java").contains("lop"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXlangX_groupByXa_lang_1_countX() {
        final Traversal<Vertex, Map<String, Integer>> traversal = get_g_V_hasXlangX_groupByXlang_1_sizeX();
        System.out.println("Testing: " + traversal);
        final Map<String, Integer> map = traversal.next();
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(Integer.valueOf(2), map.get("java"));
        assertFalse(traversal.hasNext());
    }

    public static class JavaGroupByTest extends GroupByTest {
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_groupByXnameX() {
            return (Traversal) g.V().groupBy(v -> v.value("name"));
        }

        public Traversal<Vertex, Map<String, List<String>>> get_g_V_hasXlangX_groupByXa_lang_nameX_out_capXaX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .groupBy("a",
                            v -> v.value("lang"),
                            v -> v.value("name")).out().cap("a");
        }

        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .groupBy(v -> v.value("lang"),
                            v -> 1,
                            vv -> vv.size());
        }
    }

    public static class JavaComputerGroupByTest extends GroupByTest {
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_groupByXnameX() {
            return (Traversal) g.V().groupBy(v -> v.value("name")).submit(g.compute());
        }

        public Traversal<Vertex, Map<String, List<String>>> get_g_V_hasXlangX_groupByXa_lang_nameX_out_capXaX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .groupBy("a",
                            v -> v.value("lang"),
                            v -> v.value("name")).out().cap("a").submit(g.compute());
        }

        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .groupBy(v -> v.value("lang"),
                            v -> 1,
                            vv -> vv.size()).submit(g.compute());
        }
    }

}
