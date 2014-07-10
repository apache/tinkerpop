package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class CountTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Long> get_g_V_count();

    public abstract Traversal<Vertex, Long> get_g_V_filterXfalseX_count();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_count();
        System.out.println("Testing: " + traversal);
        assertEquals(new Long(6), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @Ignore
    @LoadGraphWith(CLASSIC)
    public void g_V_filterXfalseX_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_filterXfalseX_count();
        System.out.println("Testing: " + traversal);
        assertEquals(new Long(0), traversal.next());
        assertFalse(traversal.hasNext());
    }

    public static class JavaCountTest extends CountTest {
        public Traversal<Vertex, Long> get_g_V_count() {
            return g.V().count();
        }

        public Traversal<Vertex, Long> get_g_V_filterXfalseX_count() {
            return g.V().filter(v -> false).count();
        }
    }

    public static class JavaComputerCountTest extends CountTest {
        public Traversal<Vertex, Long> get_g_V_count() {
            return g.V().count().submit(g.compute());
        }

        public Traversal<Vertex, Long> get_g_V_filterXfalseX_count() {
            return g.V().filter(v -> false).count().submit(g.compute());
        }
    }
}
