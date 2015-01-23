package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class ChooseTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name();

    public abstract Traversal<Vertex, String> get_g_VX1X_chooseX0X_forkX0__outX_name(Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_hasXlabel_personX_chooseXname_lengthX_forkX5__inX_forkX4__outX_forkX3__bothX_name();

    public abstract Traversal<Vertex, Object> get_g_V_chooseXout_count_nextX_forkX2L__nameX_forkX3L__valueMapX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXname_length_5XoutXinX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_chooseXname_length_5XoutXinX_name();
        printTraversalForm(traversal);
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
    @LoadGraphWith(MODERN)
    public void g_VX1X_chooseX0X_forkX0__outX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_chooseX0X_forkX0__outX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
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
    @LoadGraphWith(MODERN)
    public void g_V_hasXlabel_personX_chooseXname_lengthX_forkX5__inX_forkX4__outX_forkX3__bothX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_hasXlabel_personX_chooseXname_lengthX_forkX5__inX_forkX4__outX_forkX3__bothX_name();
        printTraversalForm(traversal);
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
    @LoadGraphWith(MODERN)
    public void g_V_chooseXout_count_nextX_forkX2L__nameX_forkX3L__valueMapX() {
        final Traversal<Vertex, Object> traversal = get_g_V_chooseXout_count_nextX_forkX2L__nameX_forkX3L__valueMapX();
        printTraversalForm(traversal);
        Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next().toString(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(2, counter);
        assertEquals(2, counts.size());
        assertEquals(Long.valueOf(1), counts.get("{name=[marko], age=[29]}"));
        assertEquals(Long.valueOf(1), counts.get("josh"));
    }

    public static class StandardTest extends ChooseTest {

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name() {
            return g.V().choose(v -> v.<String>value("name").length() == 5,
                    __.out(),
                    __.in()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_chooseX0X_forkX0__outX_name(Object v1Id) {
            return g.V(v1Id).choose(t -> 0).fork(0, __.out()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXlabel_personX_chooseXname_lengthX_forkX5__inX_forkX4__outX_forkX3__bothX_name() {
            return g.V().has(T.label, "person").choose(v -> v.<String>value("name").length())
                    .fork(5, __.in())
                    .fork(4, __.out())
                    .fork(3, __.both()).values("name");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_count_nextX_forkX2L__nameX_forkX3L__valueMapX() {
            return g.V().choose(v -> v.out().count().next())
                    .fork(2L, __.values("name"))
                    .fork(3L, __.valueMap());
        }
    }

    public static class ComputerTest extends ChooseTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name() {
            return g.V().choose(v -> v.<String>value("name").length() == 5,
                    __.out(),
                    __.in()).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_chooseX0X_forkX0__outX_name(Object v1Id) {
            return g.V(v1Id).choose(t -> 0).fork(0, __.out()).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXlabel_personX_chooseXname_lengthX_forkX5__inX_forkX4__outX_forkX3__bothX_name() {
            return g.V().has(T.label, "person").choose(v -> v.<String>value("name").length())
                    .fork(5, __.in())
                    .fork(4, __.out())
                    .fork(3, __.both()).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_count_nextX_forkX2L__nameX_forkX3L__valueMapX() {
            return g.V().choose(v -> v.out().count().next())
                    .fork(2L, __.values("name"))
                    .fork(3L, __.valueMap()).submit(g.compute());
        }
    }
}