package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class GroupTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX();

    public abstract Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX();

    public abstract Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_group_byXlangX_byX1X_byXsizeX();

    public abstract Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupXaX_byXnameX_byXitX_byXsizeX_jumpXx_2X_capXaX();

    public abstract Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupXaX_byXnameX_byXitX_byXsizeX_jumpXx_loops_lt_2X_capXaX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupByXnameX() {
        final Traversal<Vertex, Map<String, Collection<Vertex>>> traversal = get_g_V_group_byXnameX();
        printTraversalForm(traversal);
        final Map<String, Collection<Vertex>> map = traversal.next();
        assertEquals(6, map.size());
        map.forEach((key, values) -> {
            assertEquals(1, values.size());
            assertEquals(convertToVertexId(key), values.iterator().next().id());
        });
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXlangX_groupByXa_lang_nameX_out_capXaX() {
        final Traversal<Vertex, Map<String, Collection<String>>> traversal = get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX();
        printTraversalForm(traversal);
        final Map<String, Collection<String>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(2, map.get("java").size());
        assertTrue(map.get("java").contains("ripple"));
        assertTrue(map.get("java").contains("lop"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXlangX_groupByXlang_1_sizeX() {
        final Traversal<Vertex, Map<String, Integer>> traversal = get_g_V_hasXlangX_group_byXlangX_byX1X_byXsizeX();
        printTraversalForm(traversal);
        final Map<String, Integer> map = traversal.next();
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(Integer.valueOf(2), map.get("java"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_2X_capXaX() {
        List<Traversal<Vertex, Map<String, Integer>>> traversals = new ArrayList<>();
        traversals.add(get_g_V_asXxX_out_groupXaX_byXnameX_byXitX_byXsizeX_jumpXx_2X_capXaX());
        traversals.add(get_g_V_asXxX_out_groupXaX_byXnameX_byXitX_byXsizeX_jumpXx_loops_lt_2X_capXaX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
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

    public static class StandardTest extends GroupTest {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX() {
            return (Traversal) g.V().group().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .group("a").by("lang").by("name").out().cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_group_byXlangX_byX1X_byXsizeX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .group().by("lang").by(v -> 1).<Collection>by(Collection::size);
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupXaX_byXnameX_byXitX_byXsizeX_jumpXx_2X_capXaX() {
            return g.V().as("x").out().group("a").by("name").by(Function.identity()).<Collection>by(Collection::size).jump("x", 2).cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupXaX_byXnameX_byXitX_byXsizeX_jumpXx_loops_lt_2X_capXaX() {
            return g.V().as("x").out().group("a").by("name").by(v -> v).<Collection>by(Collection::size).jump("x", t -> t.loops() < 2).cap("a");
        }
    }

    public static class ComputerTest extends GroupTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX() {
            return (Traversal) g.V().group().by("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .group("a").by("lang").by("name").out().cap("a").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_group_byXlangX_byX1X_byXsizeX() {
            return (Traversal) g.V().<Vertex>has("lang")
                    .group().by("lang").by(v -> 1).<Collection>by(Collection::size).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupXaX_byXnameX_byXitX_byXsizeX_jumpXx_2X_capXaX() {
            return g.V().as("x").out().group("a").by("name").by(Function.identity()).<Collection>by(Collection::size).jump("x", 2).<Map<String, Integer>>cap("a").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupXaX_byXnameX_byXitX_byXsizeX_jumpXx_loops_lt_2X_capXaX() {
            return g.V().as("x").out().group("a").by("name").by(v -> v).<Collection>by(Collection::size).jump("x", t -> t.loops() < 2).<Map<String, Integer>>cap("a").submit(g.compute());
        }
    }

}
