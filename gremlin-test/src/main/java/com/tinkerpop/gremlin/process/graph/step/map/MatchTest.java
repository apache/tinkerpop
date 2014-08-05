package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.match.Bindings;
import com.tinkerpop.gremlin.process.graph.step.util.As;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MatchTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX();

    public abstract Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX();

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_c__c_out_jump2_cX_selectXnameX();

    public abstract Traversal<Vertex, String> get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_created_b__b_0created_aX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX();

    //TODO: with traversal.reversal()
    //public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__c_created_bX_selectXnameX();

    //TODO: with traversal.reversal()
    // public abstract Traversal<Vertex, String> get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_out_bX() throws Exception {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_matchXa_out_bX();
        System.out.println("Testing: " + traversal);
        assertResults(vertexToStr, traversal,
                new Bindings<Vertex>().put("a", convertToVertex(g, "marko")).put("b", convertToVertex(g, "lop")),
                new Bindings<Vertex>().put("a", convertToVertex(g, "marko")).put("b", convertToVertex(g, "josh")),
                new Bindings<Vertex>().put("a", convertToVertex(g, "marko")).put("b", convertToVertex(g, "vadas")),
                new Bindings<Vertex>().put("a", convertToVertex(g, "josh")).put("b", convertToVertex(g, "ripple")),
                new Bindings<Vertex>().put("a", convertToVertex(g, "josh")).put("b", convertToVertex(g, "lop")),
                new Bindings<Vertex>().put("a", convertToVertex(g, "peter")).put("b", convertToVertex(g, "lop")));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_out_bX_selectXb_idX() throws Exception {
        final Traversal<Vertex, Object> traversal = get_g_V_matchXa_out_bX_selectXb_idX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        final Object vadasId = convertToVertexId("vadas");
        final Object joshId = convertToVertexId("josh");
        final Object lopId = convertToVertexId("lop");
        final Object rippleId = convertToVertexId("ripple");
        Map<Object, Long> idCounts = new HashMap<>();
        while (traversal.hasNext()) {
            counter++;
            MapHelper.incr(idCounts, traversal.next(), 1l);
        }
        assertFalse(traversal.hasNext());
        assertEquals(idCounts.get(vadasId), Long.valueOf(1l));
        assertEquals(idCounts.get(lopId), Long.valueOf(3l));
        assertEquals(idCounts.get(joshId), Long.valueOf(1l));
        assertEquals(idCounts.get(rippleId), Long.valueOf(1l));
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_a_outXknowsX_b__b_outXcreatedX_c() throws Exception {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_matchXa_knows_b__b_created_cX();
        System.out.println("Testing: " + traversal);
        assertResults(vertexToStr, traversal,
                new Bindings<Vertex>().put("a", convertToVertex(g, "marko")).put("b", convertToVertex(g, "josh")).put("c", convertToVertex(g, "lop")),
                new Bindings<Vertex>().put("a", convertToVertex(g, "marko")).put("b", convertToVertex(g, "josh")).put("c", convertToVertex(g, "ripple")));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX();
        System.out.println("Testing: " + traversal);
        assertTrue(traversal.hasNext());
        assertResults(Function.identity(), traversal, new Bindings<String>().put("a", "marko").put("b", "lop"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_created_lop_b__b_0created_29_c__c_out_jump2_cX_selectXnameX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_created_lop_b__b_0created_29_c__c_out_jump2_cX_selectXnameX();
        System.out.println("Testing: " + traversal);
        assertResults(Function.identity(), traversal,
                new Bindings<String>().put("a", "marko").put("b", "lop").put("c", "marko"),
                new Bindings<String>().put("a", "josh").put("b", "lop").put("c", "marko"),
                new Bindings<String>().put("a", "peter").put("b", "lop").put("c", "marko"),
                // TODO: Below 3 results are being repeated twice
                new Bindings<String>().put("a", "marko").put("b", "lop").put("c", "marko"),
                new Bindings<String>().put("a", "josh").put("b", "lop").put("c", "marko"),
                new Bindings<String>().put("a", "peter").put("b", "lop").put("c", "marko"));
        // TODO: Above 3 results should not be needed
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name() throws Exception {
        final Traversal<Vertex, String> traversal = get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name();
        System.out.println("Testing: " + traversal);
        assertEquals("lop", traversal.next());
        assertEquals("lop", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_created_b__b_0created_aX() throws Exception {
        try {
            get_g_V_matchXa_created_b__b_0created_aX();
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX();
        System.out.println("Testing: " + traversal);
        assertResults(Function.identity(), traversal,
                new Bindings<String>().put("a", "marko").put("b", "josh").put("c", "josh"),
                new Bindings<String>().put("a", "marko").put("b", "josh").put("c", "marko"),
                new Bindings<String>().put("a", "marko").put("b", "josh").put("c", "peter"),
                // TODO: Results are being repeated twice
                new Bindings<String>().put("a", "marko").put("b", "josh").put("c", "marko"));
    }

    /* TODO: this test requires path reversal
    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_created_b__c_created_bX_selectXnameX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_created_b__c_created_bX_selectXnameX();
        System.out.println("Testing: " + traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Long> countMap = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, String> bindings = traversal.next();
            // TODO: c is not being bound
            // assertEquals(3, bindings.size());
            assertEquals("lop", bindings.get("b"));
            MapHelper.incr(countMap, bindings.get("a") + ":" + bindings.get("c"), 1l);
        }
        // TODO: without 'c' binding, cant check results
        // assertEquals(Long.valueOf(1), countMap.get("marko:marko"));
        //assertEquals(Long.valueOf(1), countMap.get("marko:josh"));
        //assertEquals(Long.valueOf(1), countMap.get("marko:peter"));
        //assertEquals(Long.valueOf(1), countMap.get("josh:marko"));
        //assertEquals(Long.valueOf(1), countMap.get("josh:josh"));
        //assertEquals(Long.valueOf(1), countMap.get("josh:peter"));
        //assertEquals(Long.valueOf(1), countMap.get("peter:marko"));
        //assertEquals(Long.valueOf(1), countMap.get("peter:josh"));
        //assertEquals(Long.valueOf(1), countMap.get("peter:peter"));
        //assertEquals(countMap.size(), 9);
        //assertEquals(9, counter);
        assertFalse(traversal.hasNext());
    }
    */

    /* TODO: this test requires path reversal
    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name() throws Exception {
        // TODO: Doesn't work, only bindings to 'a' in binding set.
        final Traversal<Vertex, String> traversal = get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name();
        System.out.println("Testing: " + traversal);
        assertTrue(traversal.hasNext());
        final List<String> results = traversal.toList();
        assertEquals(2, results.size());
        assertTrue(results.contains("josh"));
        assertTrue(results.contains("vadas"));
    }
    */

    public static class JavaMapTest extends MatchTest {
        public JavaMapTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX() {
            return g.V().match("a", g.of().as("a").out().as("b"));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX() {
            return g.V().match("a", g.of().as("a").out().as("b")).select("b", v -> ((Vertex) v).id());
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX() {
            return g.V().match("a",
                    g.of().as("a").out("knows").as("b"),
                    g.of().as("b").out("created").as("c"));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX() {
            return g.V().match("a",
                    g.of().as("a").out("created").as("b"),
                    g.of().as("a").out().jump("a", 2).as("b")).select(As.of("a", "b"), v -> ((Vertex) v).value("name"));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_c__c_out_jump2_cX_selectXnameX() {
            return g.V().match("a",
                    g.of().as("a").out("created").has("name", "lop").as("b"),
                    g.of().as("b").in("created").has("age", 29).as("c"),
                    g.of().as("c").out().jump("c", v -> v.getLoops() < 2)).select(v -> ((Vertex) v).value("name"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name() {
            return g.V().out().out().match("a",
                    g.of().as("a").in("created").as("b"),
                    g.of().as("b").in("knows").as("c")).select("c").out("created").value("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_created_b__b_0created_aX() {
            return g.V().match("a",
                    g.of().as("a").out("created").as("b"),
                    g.of().as("b").in("created").as("a"));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX() {
            return g.V().match("a",
                    g.of().as("a").out("knows").as("b"),
                    g.of().as("b").out("created").has("name", "lop"),
                    g.of().as("b").match("a1",
                            g.of().as("a1").out("created").as("b1"),
                            g.of().as("b1").in("created").as("c1")).select("c1").as("c")).select(v -> ((Vertex) v).value("name"));
        }

        /*@Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__c_created_bX_selectXnameX() {
            return g.V().match("a",
                    g.of().as("a").out("created").as("b"),
                    g.of().as("c").out("created").as("b")).select(v -> ((Vertex) v).value("name"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name() {
            return g.V().out().out().match("a",
                    g.of().as("b").out("created").as("a"),
                    g.of().as("c").out("knows").as("b")).select("c").out("knows").value("name");
        }*/

    }

    public static class JavaComputerMapTest extends MatchTest {
        public JavaComputerMapTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX() {
            return (Traversal) g.V().match("a", g.of().as("a").out().as("b")).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX() {
            return g.V().match("a", g.of().as("a").out().as("b")).select("b", v -> ((Vertex) v).id()).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX() {
            return (Traversal) g.V().match("a",
                    g.of().as("a").out("knows").as("b"),
                    g.of().as("b").out("created").as("c")).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX() {
            return (Traversal) g.V().match("a",
                    g.of().as("a").out("created").as("b"),
                    g.of().as("a").out().jump("a", 2).as("b")).select(As.of("a", "b"), v -> ((Vertex) v).value("name")).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_c__c_out_jump2_cX_selectXnameX() {
            return (Traversal) g.V().match("a",
                    g.of().as("a").out("created").has("name", "lop").as("b"),
                    g.of().as("b").in("created").has("age", 29).as("c"),
                    g.of().as("c").out().jump("c", v -> v.getLoops() < 2)).select(v -> ((Vertex) v).value("name")).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name() {
            return (Traversal) g.V().out().out().match("a",
                    g.of().as("a").in("created").as("b"),
                    g.of().as("b").in("knows").as("c")).select("c").out("created").value("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_created_b__b_0created_aX() {
            return g.V().match("a",
                    g.of().as("a").out("created").as("b"),
                    g.of().as("b").in("created").as("a")).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX() {
            return (Traversal) g.V().match("a",
                    g.of().as("a").out("knows").as("b"),
                    g.of().as("b").out("created").has("name", "lop"),
                    g.of().as("b").match("a1",
                            g.of().as("a1").out("created").as("b1"),
                            g.of().as("b1").in("created").as("c1")).select("c1").as("c")).select(v -> ((Vertex) v).value("name")).submit(g.compute());
        }

        /*@Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__c_created_bX_selectXnameX() {
            // TODO: Does not work with GraphComputer (to recheck, add .submit(g.compute())
            return g.V().match("a",
                    g.of().as("a").out("created").as("b"),
                    g.of().as("c").out("created").as("b")).select(v -> ((Vertex) v).value("name"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name() {
            return (Traversal) g.V().out().out().match("a",
                    g.of().as("b").out("created").as("a"),
                    g.of().as("c").out("knows").as("b")).select("c").out("knows").value("name").submit(g.compute());
        }*/
    }

    private static final Function<Vertex, String> vertexToStr = v -> v.id().toString();

    private <S, E> void assertResults(final Function<E, String> toStringFunction,
                                      final Traversal<S, Map<String, E>> actual,
                                      final Bindings<E>... expected) {
        Comparator<Bindings<E>> comp = new Bindings.BindingsComparator<>(toStringFunction);

        List<Bindings<E>> actualList = toBindings(actual);
        List<Bindings<E>> expectedList = new LinkedList<>();
        Collections.addAll(expectedList, expected);

        if (expectedList.size() > actualList.size()) {
            fail("" + (expectedList.size() - actualList.size()) + " expected results not found, including " + expectedList.get(actualList.size()));
        } else if (actualList.size() > expectedList.size()) {
            fail("" + (actualList.size() - expectedList.size()) + " unexpected results, including " + actualList.get(expectedList.size()));
        }

        Collections.sort(actualList, comp);
        Collections.sort(expectedList, comp);

        for (int j = 0; j < actualList.size(); j++) {
            Bindings<E> a = actualList.get(j);
            Bindings<E> e = expectedList.get(j);

            if (0 != comp.compare(a, e)) {
                fail("unexpected result(s), including " + a);
            }
        }
        assertFalse(actual.hasNext());
    }

    private <S, E> List<Bindings<E>> toBindings(final Traversal<S, Map<String, E>> traversal) {
        List<Bindings<E>> result = new LinkedList<>();
        traversal.forEach(o -> {
            result.add(new Bindings<>(o));
        });
        return result;
    }
}
