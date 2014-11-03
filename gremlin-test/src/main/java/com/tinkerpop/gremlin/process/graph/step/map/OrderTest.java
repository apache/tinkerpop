package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class OrderTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_name_order();

    public abstract Traversal<Vertex, String> get_g_V_name_orderXabX();

    public abstract Traversal<Vertex, String> get_g_V_name_orderXa1_b1__b2_a2X();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_name_order() {
        final Traversal<Vertex, String> traversal = get_g_V_name_order();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(names.size(), 6);
        assertEquals("josh", names.get(0));
        assertEquals("lop", names.get(1));
        assertEquals("marko", names.get(2));
        assertEquals("peter", names.get(3));
        assertEquals("ripple", names.get(4));
        assertEquals("vadas", names.get(5));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_name_orderXabX() {
        final Traversal<Vertex, String> traversal = get_g_V_name_orderXabX();
        printTraversalForm(traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(names.size(), 6);
        assertEquals("josh", names.get(5));
        assertEquals("lop", names.get(4));
        assertEquals("marko", names.get(3));
        assertEquals("peter", names.get(2));
        assertEquals("ripple", names.get(1));
        assertEquals("vadas", names.get(0));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_name_orderXa1_b1__a2_b2X() {
        final Traversal<Vertex, String> traversal = get_g_V_name_orderXa1_b1__b2_a2X();
        printTraversalForm(traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(names.size(), 6);
        assertEquals("marko", names.get(0));
        assertEquals("vadas", names.get(1));
        assertEquals("peter", names.get(2));
        assertEquals("ripple", names.get(3));
        assertEquals("josh", names.get(4));
        assertEquals("lop", names.get(5));
    }


    public static class StandardTest extends OrderTest {

        @Override
        public Traversal<Vertex, String> get_g_V_name_order() {
            return g.V().<String>values("name").order();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_orderXabX() {
            return g.V().<String>values("name").order((a, b) -> b.get().compareTo(a.get()));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_orderXa1_b1__b2_a2X() {
            return g.V().<String>values("name")
                    .order((a, b) -> a.get().substring(1, 2).compareTo(b.get().substring(1, 2)),
                            (a, b) -> b.get().substring(2, 3).compareTo(a.get().substring(2, 3)));
        }

    }

    public static class ComputerTest extends OrderTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_order() {
            return g.V().<String>values("name").order().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_orderXabX() {
            return g.V().<String>values("name").order((a, b) -> b.get().compareTo(a.get())).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_orderXa1_b1__b2_a2X() {
            return g.V().<String>values("name")
                    .order((a, b) -> a.get().substring(1, 2).compareTo(b.get().substring(1, 2)),
                            (a, b) -> b.get().substring(2, 3).compareTo(a.get().substring(2, 3))).submit(g.compute());
        }
    }
}
