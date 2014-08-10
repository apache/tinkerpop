package com.tinkerpop.gremlin.process.graph.step.sideEffect;

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
    public abstract Traversal<Vertex, String> get_g_v1_sideEffectXstore_aX_valueXnameX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_v1_out_sideEffectXincr_cX_valueXnameX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_v1_out_sideEffectXX_valueXnameX(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_sideEffectXstore_aX_valueXnameX() {
        final Traversal<Vertex, String> traversal = get_g_v1_sideEffectXstore_aX_valueXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals(traversal.next(), "marko");
        assertFalse(traversal.hasNext());
        assertEquals(convertToVertexId("marko"), traversal.memory().<List<Vertex>>get("a").get().get(0).id());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_sideEffectXincr_cX_valueXnameX() {
        final Traversal<Vertex, String> traversal = get_g_v1_out_sideEffectXincr_cX_valueXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(traversal);
        assertEquals(new Integer(3), traversal.memory().<List<Integer>>get("c").get().get(0));
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
        final Traversal<Vertex, String> traversal = get_g_v1_out_sideEffectXX_valueXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(traversal);
    }

    public static class JavaSideEffectTest extends SideEffectTest {
        public Traversal<Vertex, String> get_g_v1_sideEffectXstore_aX_valueXnameX(final Object v1Id) {
            final List<Vertex> a = new ArrayList<>();
            return g.v(v1Id).with("a", a).sideEffect(traverser -> {
                a.clear();
                a.add(traverser.get());
            }).value("name");
        }

        public Traversal<Vertex, String> get_g_v1_out_sideEffectXincr_cX_valueXnameX(final Object v1Id) {
            final List<Integer> c = new ArrayList<>();
            c.add(0);
            return g.v(v1Id).with("c", c).out().sideEffect(traverser -> {
                Integer temp = c.get(0);
                c.clear();
                c.add(temp + 1);
            }).value("name");
        }

        public Traversal<Vertex, String> get_g_v1_out_sideEffectXX_valueXnameX(final Object v1Id) {
            return g.v(v1Id).out().sideEffect(traverser -> {
            }).value("name");
        }
    }
}
