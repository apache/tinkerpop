package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
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
 */
public abstract class UnionTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_unionXout__inX_name();

    public abstract Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX_groupCount();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount();

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

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX_groupCount() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX_groupCount();
        printTraversalForm(traversal);
        final Map<String, Long> groupCount = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(3l, groupCount.get("lop").longValue());
        assertEquals(1l, groupCount.get("ripple").longValue());
        assertEquals(4l, groupCount.get("java").longValue());
        assertEquals(1l, groupCount.get("josh").longValue());
        assertEquals(1l, groupCount.get("vadas").longValue());
        assertEquals(4l, groupCount.get("person").longValue());
        assertEquals(6, groupCount.size());

    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount();
        printTraversalForm(traversal);
        final Map<String, Long> groupCount = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(12l, groupCount.get("software").longValue());
        assertEquals(20l, groupCount.get("person").longValue());
        assertEquals(2, groupCount.size());

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

        public Traversal<Vertex, Map<String, Long>> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX_groupCount() {
            return (Traversal) g.V().choose(v -> v.label().equals("person"), __.union(__.out().values("lang"), __.out().values("name")), __.in().label()).groupCount();
        }

        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() {
            return (Traversal) g.V().union(
                    __.repeat(__.union(
                            __.out("created"),
                            __.in("created"))).times(2),
                    __.repeat(__.union(
                            __.in("created"),
                            __.out("created"))).times(2)).label().groupCount();
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

        public Traversal<Vertex, Map<String, Long>> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX_groupCount() {
            return (Traversal) g.V().choose(v -> v.label().equals("person"), __.union(__.out().values("lang"), __.out().values("name")), __.in().label()).groupCount().submit(g.compute());
        }

        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() {
            return (Traversal) g.V().union(
                    __.repeat(__.union(
                            __.out("created"),
                            __.in("created"))).times(2),
                    __.repeat(__.union(
                            __.in("created"),
                            __.out("created"))).times(2)).label().groupCount().submit(g.compute());
        }
    }
}
