package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class UnionTest extends AbstractGremlinTest {

    /*public abstract Traversal<Vertex, String> get_g_V_unionXout_inX_name();

    @Test
    @LoadGraphWith(CLASSIC_DOUBLE)
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

    public static class JavaUnionTest extends UnionTest {

        public Traversal<Vertex, String> get_g_V_unionXout_inX_name() {
            return g.V().union(g.of().out(), g.of().in()).value("name");
        }
    }

    public static class JavaComputerUnionTest extends UnionTest {

        public Traversal<Vertex, String> get_g_V_unionXout_inX_name() {
            return g.V().union(g.of().out(), g.of().in()).<String>value("name").submit(g.compute());
        }
    }*/
}
