package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
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

    public abstract Traversal<Vertex, Map<String, List<String>>> get_g_V_hasXlangX_groupByXlang_nameX_asXaX_out_capXaX();

    public abstract Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX();

    public abstract Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX();

    public abstract Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_loops_lt_2X_capXaX();

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

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXlangX_groupByXlang_nameX_asXaX_out_capXaX() {
        final Traversal<Vertex, Map<String, List<String>>> traversal = get_g_V_hasXlangX_groupByXlang_nameX_asXaX_out_capXaX();
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
    public void g_V_hasXlangX_groupByXlang_1_sizeX() {
        final Traversal<Vertex, Map<String, Integer>> traversal = get_g_V_hasXlangX_groupByXlang_1_sizeX();
        System.out.println("Testing: " + traversal);
        final Map<String, Integer> map = traversal.next();
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(Integer.valueOf(2), map.get("java"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX() {
        List<Traversal<Vertex, Map<String, Integer>>> traversals = new ArrayList<>();
        traversals.add(get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX());
        traversals.add(get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_loops_lt_2X_capXaX());
        traversals.forEach(traversal -> {
            System.out.println("Testing: " + traversal);
            final Map<String, Integer> map = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(4, map.size());
            assertTrue(map.containsKey("vadas"));
            assertEquals(Integer.valueOf(1), map.get("vadas"));
            assertTrue(map.containsKey("josh"));
            assertEquals(Integer.valueOf(1), map.get("josh"));
            assertTrue(map.containsKey("lop"));
            assertEquals(Integer.valueOf(4), map.get("lop"));
            assertTrue(map.containsKey("ripple"));
            assertEquals(Integer.valueOf(2), map.get("ripple"));
        });
    }

    public static class JavaGroupByTest extends GroupByTest {
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_groupByXnameX() {
            return (Traversal) g.V().groupBy(v -> v.value("name"));
        }

        public Traversal<Vertex, Map<String, List<String>>> get_g_V_hasXlangX_groupByXlang_nameX_asXaX_out_capXaX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .groupBy(v -> v.value("lang"),
                            v -> v.value("name")).as("a").out().cap("a");
        }

        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .groupBy(v -> v.value("lang"),
                            v -> 1,
                            vv -> vv.size());
        }

        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX() {
            return g.V().as("x").out().groupBy(v -> v.value("name"), v -> v, vv -> vv.size()).as("a").jump("x", 2).cap("a");
        }

        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_loops_lt_2X_capXaX() {
            return g.V().as("x").out().groupBy(v -> v.value("name"), v -> v, vv -> vv.size()).as("a").jump("x", t -> t.getLoops() < 2).cap("a");
        }
    }

    public static class JavaComputerGroupByTest extends GroupByTest {
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_groupByXnameX() {
            return (Traversal) g.V().groupBy(v -> v.value("name")).submit(g.compute());
        }

        public Traversal<Vertex, Map<String, List<String>>> get_g_V_hasXlangX_groupByXlang_nameX_asXaX_out_capXaX() {
            // TODO: This doesn't work with GraphComputer cause there are no results so you get a NoSuchElementException
            return (Traversal) g.V().<Vertex>has("lang")
                    .groupBy(v -> v.value("lang"),
                            v -> v.value("name")).as("a").out().cap("a");
        }

        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .groupBy(v -> v.value("lang"),
                            v -> 1,
                            vv -> vv.size()).submit(g.compute());
        }

        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX() {
            return g.V().as("x").out().groupBy(v -> v.value("name"), v -> v, vv -> vv.size()).as("a").jump("x", 2).<Map<String, Integer>>cap("a").submit(g.compute());
        }

        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_loops_lt_2X_capXaX() {
            return g.V().as("x").out().groupBy(v -> v.value("name"), v -> v, vv -> vv.size()).as("a").jump("x", t -> t.getLoops() < 2).<Map<String, Integer>>cap("a").submit(g.compute());
        }
    }

}
