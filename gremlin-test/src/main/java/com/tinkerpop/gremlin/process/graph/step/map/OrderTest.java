package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class OrderTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, String> get_g_V_name_order();

    public abstract Traversal<Vertex, String> get_g_V_name_orderXabX();

    public abstract Traversal<Vertex, String> get_g_V_orderXa_nameXb_nameX_name();

    public abstract Traversal<Vertex, String> get_g_V_lang_order();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_name_order() {
        final Traversal<Vertex, String> traversal = get_g_V_name_order();
        printTraversalForm(traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
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
    public void g_V_orderXa_nameXb_nameX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_orderXa_nameXb_nameX_name();
        printTraversalForm(traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
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
    public void g_V_lang_order() {
        final Traversal<Vertex, String> traversal = get_g_V_lang_order();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("java", traversal.next());
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends OrderTest {

        @Override
        public Traversal<Vertex, String> get_g_V_name_order() {
            return g.V().<String>value("name").order();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_orderXabX() {
            return g.V().<String>value("name").order((a, b) -> b.get().compareTo(a.get()));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orderXa_nameXb_nameX_name() {
            return g.V().order((a, b) -> a.get().<String>value("name").compareTo(b.get().<String>value("name"))).value("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_lang_order() {
            return g.V().<String>value("lang").order();
        }
    }
}
