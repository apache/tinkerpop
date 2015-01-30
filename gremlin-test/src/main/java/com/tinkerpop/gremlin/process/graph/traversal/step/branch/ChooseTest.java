package com.tinkerpop.gremlin.process.graph.traversal.step.branch;

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
import static com.tinkerpop.gremlin.process.graph.traversal.__.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class ChooseTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name();

    public abstract Traversal<Vertex, String> get_g_VX1X_chooseX0X_optionX0__outX_name(Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_hasXlabel_personX_chooseXname_lengthX_optionX5__inX_optionX4__outX_optionX3__bothX_name();

    public abstract Traversal<Vertex, Object> get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX();

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
    public void g_VX1X_chooseX0X_optionX0__outX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_chooseX0X_optionX0__outX_name(convertToVertexId("marko"));
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
    public void g_V_hasXlabel_personX_chooseXname_lengthX_optionX5__inX_optionX4__outX_optionX3__bothX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_hasXlabel_personX_chooseXname_lengthX_optionX5__inX_optionX4__outX_optionX3__bothX_name();
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
    public void g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX() {
        final Traversal<Vertex, Object> traversal = get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX();
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
                    out(),
                    in()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_chooseX0X_optionX0__outX_name(Object v1Id) {
            return g.V(v1Id).choose(t -> 0).option(0, out()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXlabel_personX_chooseXname_lengthX_optionX5__inX_optionX4__outX_optionX3__bothX_name() {
            return g.V().has(T.label, "person").choose(v -> v.<String>value("name").length())
                    .option(5, in())
                    .option(4, out())
                    .option(3, both()).values("name");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX() {
            return g.V().choose(out().count())
                    .option(2L, values("name"))
                    .option(3L, valueMap());
        }
    }

    public static class ComputerTest extends ChooseTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name() {
            return g.V().choose(v -> v.<String>value("name").length() == 5,
                    out(),
                    in()).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_chooseX0X_optionX0__outX_name(Object v1Id) {
            return g.V(v1Id).choose(t -> 0).option(0, out()).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXlabel_personX_chooseXname_lengthX_optionX5__inX_optionX4__outX_optionX3__bothX_name() {
            return g.V().has(T.label, "person").choose(v -> v.<String>value("name").length())
                    .option(5, in())
                    .option(4, out())
                    .option(3, both()).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX() {
            return g.V().choose(out().count())
                    .option(2L, values("name"))
                    .option(3L, valueMap()).submit(g.compute());
        }
    }
}