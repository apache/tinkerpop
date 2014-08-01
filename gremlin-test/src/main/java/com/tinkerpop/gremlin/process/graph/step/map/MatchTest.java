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

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__c_created_bX_selectXnameX();

    public abstract Traversal<Vertex, String> get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name();

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

        /*
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Vertex> bindings = traversal.next();
            assertEquals(2, bindings.size());
            final Object aId = bindings.get("a").id();
            final Object bId = bindings.get("b").id();
            if (aId.equals(convertToVertexId("marko"))) {
                assertTrue(bId.equals(convertToVertexId("vadas")) ||
                        bId.equals(convertToVertexId("lop")) ||
                        bId.equals(convertToVertexId("josh")));
            } else if (aId.equals(convertToVertexId("josh"))) {
                assertTrue(bId.equals(convertToVertexId("lop")) ||
                        bId.equals(convertToVertexId("ripple")));
            } else if (aId.equals(convertToVertexId("peter"))) {
                assertEquals(convertToVertexId("lop"), bId);
            } else {
                assertFalse(true);
            }
        }
        assertFalse(traversal.hasNext());
        // assertEquals(6, counter);
        */
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
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Vertex> bindings = traversal.next();
            assertEquals(3, bindings.size());
            final Object aId = bindings.get("a").id();
            final Object bId = bindings.get("b").id();
            final Object cId = bindings.get("c").id();
            assertEquals(convertToVertexId("marko"), aId);
            assertEquals(convertToVertexId("josh"), bId);
            assertTrue(cId.equals(convertToVertexId("lop")) ||
                    cId.equals(convertToVertexId("ripple")));
        }
        assertFalse(traversal.hasNext());
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX();
        System.out.println("Testing: " + traversal);
        assertTrue(traversal.hasNext());
        final Map<String, String> bindings = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, bindings.size());
        assertEquals("marko", bindings.get("a"));
        assertEquals("lop", bindings.get("b"));
        assertFalse(traversal.hasNext());
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
        }

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
        }
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
    }

    private <S, E> List<Bindings<E>> toBindings(final Traversal<S, Map<String, E>> traversal) {
        List<Bindings<E>> result = new LinkedList<>();
        traversal.forEach(o -> {
            result.add(new Bindings<>(o));
        });
        return result;
    }
}
