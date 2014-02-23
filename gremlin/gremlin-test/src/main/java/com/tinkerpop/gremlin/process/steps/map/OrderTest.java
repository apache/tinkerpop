package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class OrderTest extends AbstractGremlinTest {

    public abstract Iterator<String> get_g_V_name_order();

    public abstract Iterator<String> get_g_V_name_orderXabX();

    public abstract Iterator<String> get_g_V_orderXa_nameXb_nameX_name();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_name_order() {
        final Iterator<String> step = get_g_V_name_order();
        System.out.println("Testing: " + step);
        final List<String> names = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(names.size(), 6);
        assertEquals("josh", names.get(0));
        assertEquals("lop", names.get(1));
        assertEquals("marko", names.get(2));
        assertEquals("peter", names.get(3));
        assertEquals("ripple", names.get(4));
        assertEquals("vadas", names.get(5));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_name_orderXabX() {
        final Iterator<String> step = get_g_V_name_orderXabX();
        System.out.println("Testing: " + step);
        final List<String> names = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(names.size(), 6);
        assertEquals("josh", names.get(5));
        assertEquals("lop", names.get(4));
        assertEquals("marko", names.get(3));
        assertEquals("peter", names.get(2));
        assertEquals("ripple", names.get(1));
        assertEquals("vadas", names.get(0));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_orderXa_nameXb_nameX_name() {
        final Iterator<String> step = get_g_V_orderXa_nameXb_nameX_name();
        System.out.println("Testing: " + step);
        final List<String> names = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(names.size(), 6);
        assertEquals("josh", names.get(0));
        assertEquals("lop", names.get(1));
        assertEquals("marko", names.get(2));
        assertEquals("peter", names.get(3));
        assertEquals("ripple", names.get(4));
        assertEquals("vadas", names.get(5));
    }

    public static class JavaOrderTest extends OrderTest {

        public Iterator<String> get_g_V_name_order() {
            return g.V().<String>value("name").order();
        }

        public Iterator<String> get_g_V_name_orderXabX() {
            return g.V().<String>value("name").order((a, b) -> b.get().compareTo(a.get()));
        }

        public Iterator<String> get_g_V_orderXa_nameXb_nameX_name() {
            return g.V().order((a, b) -> a.get().<String>getValue("name").compareTo(b.get().getValue("name"))).value("name");
        }
    }
}
