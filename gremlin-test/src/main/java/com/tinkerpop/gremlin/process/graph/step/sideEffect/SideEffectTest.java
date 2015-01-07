package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SideEffectTest extends AbstractGremlinTest {
    public abstract Traversal<Vertex, String> get_g_VX1X_sideEffectXstore_aX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_out_sideEffectXincr_cX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_out_sideEffectXX_name(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_sideEffectXstore_aX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_sideEffectXstore_aX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals(traversal.next(), "marko");
        assertFalse(traversal.hasNext());
        assertEquals(convertToVertexId("marko"), traversal.asAdmin().getSideEffects().<List<Vertex>>get("a").get(0).id());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_sideEffectXincr_cX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_out_sideEffectXincr_cX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(traversal);
        assertEquals(new Integer(3), traversal.asAdmin().getSideEffects().<List<Integer>>get("c").get(0));
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
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_sideEffectXX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_out_sideEffectXX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(traversal);
    }

    public static class StandardTest extends SideEffectTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_sideEffectXstore_aX_name(final Object v1Id) {
            return g.V(v1Id).withSideEffect("a", ArrayList::new).sideEffect(traverser -> {
                traverser.<List>sideEffects("a").clear();
                traverser.<List<Vertex>>sideEffects("a").add(traverser.get());
            }).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_sideEffectXincr_cX_name(final Object v1Id) {
            return g.V(v1Id).withSideEffect("c", () -> {
                final List<Integer> list = new ArrayList<>();
                list.add(0);
                return list;
            }).out().sideEffect(traverser -> {
                Integer temp = traverser.<List<Integer>>sideEffects("c").get(0);
                traverser.<List<Integer>>sideEffects("c").clear();
                traverser.<List<Integer>>sideEffects("c").add(temp + 1);
            }).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_sideEffectXX_name(final Object v1Id) {
            return g.V(v1Id).out().sideEffect(traverser -> {
            }).values("name");
        }
    }
}
