package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class UnionTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_unionXout_inX_name();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_unionXout_inX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_unionXout_inX_name();
        System.out.println("Testing: " + traversal);
        Map<String, Long> map = new HashMap<>();
        int count = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(map, traversal.next(), 1l);
            count++;
        }
        assertEquals(12, count);
        assertEquals(6, map.size());
        assertEquals(Long.valueOf(3), map.get("marko"));
        assertEquals(Long.valueOf(3), map.get("lop"));
        assertEquals(Long.valueOf(1), map.get("peter"));
        assertEquals(Long.valueOf(1), map.get("ripple"));
        assertEquals(Long.valueOf(3), map.get("josh"));
        assertEquals(Long.valueOf(1), map.get("vadas"));
    }

    public static class StandardTest extends UnionTest {

        public Traversal<Vertex, String> get_g_V_unionXout_inX_name() {
            return g.V().union(g.<Vertex>of().out(), g.<Vertex>of().in()).value("name");
        }
    }

    public static class ComputerTest extends UnionTest {

        public Traversal<Vertex, String> get_g_V_unionXout_inX_name() {
            return g.V().union(g.<Vertex>of().out(), g.<Vertex>of().in()).<String>value("name").submit(g.compute());
        }
    }
}
