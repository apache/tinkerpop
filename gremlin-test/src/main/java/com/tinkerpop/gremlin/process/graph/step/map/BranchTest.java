package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class BranchTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, String> get_g_V_branchXname_length_5XoutXinX_name();

    public abstract Traversal<Vertex, String> get_g_v1_branchX0XoutX_name(Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_hasXageX_branchXname_lengthX5_in_4_outX_name();

    public abstract Traversal<Vertex, Integer> get_g_V_valueXageX_branchXnullX27_identity_29_minus2X();

    public abstract Traversal<Vertex, Object> get_g_V_branchXout_count_nextX2L_valueXnameX_3L_valuesX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_branchXname_length_5XoutXinX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_branchXname_length_5XoutXinX_name();
        System.out.println("Testing: " + traversal);
        Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(9, counter);
        assertEquals(5, counts.size());
        assertEquals(Long.valueOf(1), counts.get("vadas"));
        assertEquals(Long.valueOf(3), counts.get("josh"));
        assertEquals(Long.valueOf(2), counts.get("lop"));
        assertEquals(Long.valueOf(2), counts.get("marko"));
        assertEquals(Long.valueOf(1), counts.get("peter"));

    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_branchX0XoutX_name() {
        final Traversal<Vertex, String> traversal = get_g_v1_branchX0XoutX_name(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(3, counter);
        assertEquals(3, counts.size());
        assertEquals(Long.valueOf(1), counts.get("vadas"));
        assertEquals(Long.valueOf(1), counts.get("josh"));
        assertEquals(Long.valueOf(1), counts.get("lop"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXageX_branchXname_lengthX5_in_4_outX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_hasXageX_branchXname_lengthX5_in_4_outX_name();
        System.out.println("Testing: " + traversal);
        Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(3, counter);
        assertEquals(3, counts.size());
        assertEquals(Long.valueOf(1), counts.get("marko"));
        assertEquals(Long.valueOf(1), counts.get("lop"));
        assertEquals(Long.valueOf(1), counts.get("ripple"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_valueXageX_branchXnullX27_identity_29_minus2X() {
        final Traversal<Vertex, Integer> traversal = get_g_V_valueXageX_branchXnullX27_identity_29_minus2X();
        System.out.println("Testing: " + traversal);
        Map<Integer, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(2, counter);
        assertEquals(1, counts.size());
        assertEquals(Long.valueOf(2), counts.get(27));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_branchXout_count_nextX2L_valueXnameX_3L_valuesX() {
        final Traversal<Vertex, Object> traversal = get_g_V_branchXout_count_nextX2L_valueXnameX_3L_valuesX();
        System.out.println("Testing: " + traversal);
        Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next().toString(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(2, counter);
        assertEquals(2, counts.size());
        assertEquals(Long.valueOf(1), counts.get("{name=marko, age=29}"));
        assertEquals(Long.valueOf(1), counts.get("josh"));
    }

    public static class JavaBranchTest extends BranchTest {

        public Traversal<Vertex, String> get_g_V_branchXname_length_5XoutXinX_name() {
            return g.V().branch(t -> t.get().<String>value("name").length() == 5,
                    g.of().out(),
                    g.of().in()).value("name");
        }

        public Traversal<Vertex, String> get_g_v1_branchX0XoutX_name(Object v1Id) {
            return g.v(v1Id).branch(t -> 0, new HashMap() {{
                put(0, g.of().out().value("name"));
            }});
        }

        public Traversal<Vertex, String> get_g_V_hasXageX_branchXname_lengthX5_in_4_outX_name() {
            return g.V().has("age").branch(t -> t.get().<String>value("name").length(), new HashMap() {{
                put(4, g.of().out());
                put(5, g.of().in());
            }}).value("name");
        }

        public Traversal<Vertex, Integer> get_g_V_valueXageX_branchXnullX27_identity_29_minus2X() {
            return g.V().value("age").branch(null, new HashMap() {{
                put(27, g.of().identity());
                put(29, g.of().map(t -> (Integer) t.get() - 2));
            }});
        }

        public Traversal<Vertex, Object> get_g_V_branchXout_count_nextX2L_valueXnameX_3L_valuesX() {
            return g.V().branch(t -> t.get().out().count().next(), new HashMap() {{
                put(2L, g.of().value("name"));
                put(3L, g.of().values());
            }});
        }
    }
}