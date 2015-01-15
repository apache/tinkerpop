package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class UnionTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_unionXout__inX_name();

    public abstract Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_unionXout__inX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_unionXout__inX_name();
        printTraversalForm(traversal);
        checkResults(new HashMap<String, Long>() {{
            put("marko", 3l);
            put("lop", 3l);
            put("peter", 1l);
            put("ripple", 1l);
            put("josh", 3l);
            put("vadas", 1l);
        }}, traversal);
    }

    @Test
    @Ignore("Need to de-linearize RepeatStep")
    @LoadGraphWith(MODERN)
    public void g_VX1X_unionXrepeatXoutX_timesX2X__outX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(new HashMap<String, Long>() {{
            put("lop", 2l);
            put("ripple", 1l);
            put("josh", 1l);
            put("vadas", 1l);
        }}, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX() {
        final Traversal<Vertex, String> traversal = get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX();
        printTraversalForm(traversal);
        checkResults(new HashMap<String, Long>() {{
            put("lop", 3l);
            put("ripple", 1l);
            put("java", 4l);
            put("josh", 1l);
            put("vadas", 1l);
            put("person", 4l);
        }}, traversal);
    }

    public static class StandardTest extends UnionTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex, String> get_g_V_unionXout__inX_name() {
            return g.V().union(__.out(), __.in()).values("name");
        }

        public Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id) {
            return g.V(v1Id).union(__.repeat(__.out()).times(2), __.out()).values("name");
        }

        public Traversal<Vertex, String> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX() {
            return g.V().choose(v -> v.label().equals("person"), __.union(__.out().values("lang"), __.out().values("name")), __.in().label());
        }
    }

    public static class ComputerTest extends UnionTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex, String> get_g_V_unionXout__inX_name() {
            return g.V().union(__.out(), __.in()).<String>values("name").submit(g.compute());
        }

        public Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id) {
            return g.V(v1Id).union(__.repeat(__.out()).times(2), __.out()).<String>values("name").submit(g.compute());
        }

        public Traversal<Vertex, String> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX() {
            return g.V().choose(v -> v.label().equals("person"), __.union(__.out().values("lang"), __.out().values("name")), __.in().label()).submit(g.compute());
        }
    }
}
