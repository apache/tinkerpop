package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class GroupCountTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_groupCountXnameX();

    public abstract Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCount();

    public abstract Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCountXaX();

    public abstract Traversal<Vertex, Map<Object, Long>> get_g_V_filterXfalseX_groupCount();

    public abstract Traversal<Vertex, Map<Object, Long>> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_capXaX();

    public abstract Traversal<Vertex, Map<Object, Long>> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_2X_capXaX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_groupCountXnameX() {
        final Traversal<Vertex, Map<Object, Long>> traversal = get_g_V_outXcreatedX_groupCountXnameX();
        printTraversalForm(traversal);
        final Map<Object, Long> map = traversal.next();
        assertEquals(map.size(), 2);
        assertEquals(Long.valueOf(3l),map.get("lop"));
        assertEquals(Long.valueOf(1l),map.get("ripple"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_name_groupCount() {
        Arrays.asList(get_g_V_outXcreatedX_name_groupCount(), get_g_V_outXcreatedX_name_groupCountXaX()).forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<Object, Long> map = traversal.next();
            assertEquals(map.size(), 2);
            assertEquals(3l,map.get("lop").longValue());
            assertEquals(1l,map.get("ripple").longValue());
            assertFalse(traversal.hasNext());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXfalseX_groupCount() {
        final Traversal<Vertex, Map<Object, Long>> traversal = get_g_V_filterXfalseX_groupCount();
        printTraversalForm(traversal);
        final Map<Object, Long> map = traversal.next();
        assertEquals(0,map.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXxX_out_groupCountXnameX_asXaX_jumpXx_2X_capXaX() {
        List<Traversal<Vertex, Map<Object, Long>>> traversals = new ArrayList<>();
        traversals.add(get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_2X_capXaX());
        traversals.add(get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_capXaX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<Object, Long> map = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(4,map.size(), 4);
            assertEquals(4l,map.get("lop").longValue());
            assertEquals(2l,map.get("ripple").longValue());
            assertEquals(1l,map.get("josh").longValue());
            assertEquals(1l,map.get("vadas").longValue());
        });
    }

    public static class StandardTest extends GroupCountTest {

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_groupCountXnameX() {
            return (Traversal) g.V().out("created").groupCount(v -> v.get().value("name"));
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCount() {
            return (Traversal) g.V().out("created").values("name").groupCount();
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCountXaX() {
            return (Traversal) g.V().out("created").values("name").groupCount("a");
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_filterXfalseX_groupCount() {
            return (Traversal) g.V().filter(t -> false).groupCount();
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_capXaX() {
            return g.V().as("x").out()
                    .groupCount("a", v -> v.get().value("name"))
                    .jump("x", h -> h.loops() < 2).cap("a");
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_2X_capXaX() {
            return g.V().as("x").out()
                    .groupCount("a", v -> v.get().value("name"))
                    .jump("x", 2).cap("a");
        }
    }

    public static class ComputerTest extends GroupCountTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_groupCountXnameX() {
            return (Traversal) g.V().out("created").groupCount(v -> v.get().value("name")).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCount() {
            return (Traversal) g.V().out("created").values("name").groupCount().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCountXaX() {
            return (Traversal) g.V().out("created").values("name").groupCount("a").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_filterXfalseX_groupCount() {
            return (Traversal) g.V().filter(t -> false).groupCount().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_capXaX() {
            return g.V().as("x").out()
                    .groupCount("a", v -> v.get().value("name"))
                    .jump("x", t -> t.loops() < 2).<Map<Object, Long>>cap("a").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_2X_capXaX() {
            return g.V().as("x").out()
                    .groupCount("a", v -> v.get().value("name"))
                    .jump("x", 2).<Map<Object, Long>>cap("a").submit(g.compute());
        }
    }
}