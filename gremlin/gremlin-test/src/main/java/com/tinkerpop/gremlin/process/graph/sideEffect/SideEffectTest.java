package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SideEffectTest extends AbstractGremlinTest {
    public abstract Traversal<Vertex, String> get_g_v1_sideEffectXstore_aX_valueXnameX();

    public abstract Traversal<Vertex, String> get_g_v1_out_sideEffectXincr_cX_valueXnameX();

    public abstract Traversal<Vertex, String> get_g_v1_out_sideEffectXX_valueXnameX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_sideEffectXstore_aX_valueXnameX() {
        final Traversal<Vertex, String> traversal = get_g_v1_sideEffectXstore_aX_valueXnameX();
        assertEquals(traversal.next(), "marko");
        assertFalse(traversal.hasNext());
        assertEquals(g.v(1), traversal.memory().<List<Vertex>>get("a").get(0));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_sideEffectXincr_cX_valueXnameX() {
        final Traversal<Vertex, String> traversal = get_g_v1_out_sideEffectXincr_cX_valueXnameX();
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(traversal);
        assertEquals(new Integer(3), traversal.memory().<List<Integer>>get("c").get(0));
    }

    private void assert_g_v1_out_sideEffectXincr_cX_valueXnameX(final Iterator<String> traversal) {
        final List<String> names = new ArrayList<>();
        while (traversal.hasNext()) {
            names.add(traversal.next());
        }
        assertEquals(3, names.size());
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("vadas"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_sideEffectXX_valueXnameX() {
        final Iterator<String> step = get_g_v1_out_sideEffectXX_valueXnameX();
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(step);
    }

    public static class JavaSideEffectTest extends SideEffectTest {
        public Traversal<Vertex, String> get_g_v1_sideEffectXstore_aX_valueXnameX() {
            final List<Vertex> a = new ArrayList<>();
            return g.v(1).with("a", a).sideEffect(holder -> {
                a.clear();
                a.add(holder.get());
            }).value("name");
        }

        public Traversal<Vertex, String> get_g_v1_out_sideEffectXincr_cX_valueXnameX() {
            final List<Integer> c = new ArrayList<>();
            c.add(0);
            return g.v(1).with("c", c).out().sideEffect(holder -> {
                Integer temp = c.get(0);
                c.clear();
                c.add(temp + 1);
            }).value("name");
        }

        public Traversal<Vertex, String> get_g_v1_out_sideEffectXX_valueXnameX() {
            return g.v(1).out().sideEffect(holder -> {
            }).value("name");
        }
    }
}
