package com.tinkerpop.gremlin.process.steps.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SideEffectTest extends AbstractGremlinTest {
    public abstract Iterator<String> get_g_v1_sideEffectXstore_aX_valueXnameX();

    public abstract Iterator<String> get_g_v1_out_sideEffectXincr_cX_valueXnameX();

    public abstract Iterator<String> get_g_v1_out_sideEffectXX_valueXnameX();

    @Test
    @LoadGraphWith(CLASSIC)
    @Ignore("Sort out vertex query stuffs with sideeffect")
    public void g_v1_sideEffectXstore_aX_valueXnameX() {
        final Iterator<String> step = get_g_v1_sideEffectXstore_aX_valueXnameX();
        assertEquals(step.next(), "marko");
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @Ignore("Sort out vertex query stuffs with sideeffect")
    public void g_v1_out_sideEffectXincr_cX_valueXnameX() {
        final Iterator<String> step = get_g_v1_out_sideEffectXincr_cX_valueXnameX();
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(step);
    }

    private void assert_g_v1_out_sideEffectXincr_cX_valueXnameX(final Iterator<String> step) {
        final List<String> names = new ArrayList<>();
        while (step.hasNext()) {
            names.add(step.next());
        }
        assertEquals(3, names.size());
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("vadas"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @Ignore("Sort out vertex query stuffs with sideeffect")
    public void g_v1_out_sideEffectXX_valueXnameX() {
        final Iterator<String> step = get_g_v1_out_sideEffectXX_valueXnameX();
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(step);
    }

    public static class JavaSideEffectTest extends SideEffectTest {
        public Iterator<String> get_g_v1_sideEffectXstore_aX_valueXnameX() {
            return null;
            //   final List<Vertex> a = new ArrayList<>();
            //   super.g_v1_sideEffectXstore_aX_valueXnameX(GremlinJ.of(g).v(1).sideEffect(holder -> {
            //       a.clear();
            //       a.add(holder.get());
            //   }).value("name"));
            //   assertEquals(g.v(1).get(), a.get(0));
        }

        public Iterator<String> get_g_v1_out_sideEffectXincr_cX_valueXnameX() {
            return null;
            //   final List<Integer> c = new ArrayList<>();
            //   c.add(0);
            //   super.g_v1_out_sideEffectXincr_cX_valueXnameX(GremlinJ.of(g).v(1).out().sideEffect(holder -> {
            //       Integer temp = c.get(0);
            //       c.clear();
            //       c.add(temp + 1);
            //    }).value("name"));
            //   assertEquals(new Integer(3), c.get(0));
        }

        public Iterator<String> get_g_v1_out_sideEffectXX_valueXnameX() {
            return null;
            //  super.g_v1_out_sideEffectXX_valueXnameX(GremlinJ.of(g).v(1).out().sideEffect(holder -> {
            //   }).value("name"));
        }
    }
}
