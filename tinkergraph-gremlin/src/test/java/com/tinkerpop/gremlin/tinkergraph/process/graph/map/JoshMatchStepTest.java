package com.tinkerpop.gremlin.tinkergraph.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.util.As;
import com.tinkerpop.gremlin.process.graph.step.map.MatchStepNew;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JoshMatchStepTest {

    @Test
    public void testNew() throws Exception {

        Graph g = TinkerFactory.createClassic();
        //System.out.println(g.v(1).outE().next());

        MatchStepNew<Object, Object> q = new MatchStepNew<>(g.V(), "a",
                g.of().as("a").out("knows").as("b"),
                g.of().as("b").out("created").as("c"));
        Predicate<Map<String, Object>> visitor = bindings -> {
            System.out.println("solution:");
            for (String key : bindings.keySet()) {
                System.out.println("\t" + key + ":\t" + bindings.get(key));
            }
            return true;
        };

        for (Vertex v : g.V().toList()) {
            q.visitSolutions(v, visitor);
        }
    }

    @Test
    public void testTreePatterns() throws Exception {
        MatchStepNew<Object, Object> query;
        Iterator iter;
        Graph g = TinkerFactory.createClassic();

        iter = g.V();
        query = new MatchStepNew<>(g.V(), "a",
                g.of().as("a").out("knows").as("b"),
                g.of().as("a").out("created").as("c"));
        assertResults(query.solve(iter),
                new Bindings<>().put("a", "v[1]").put("b", "v[2]").put("c", "v[3]"),
                new Bindings<>().put("a", "v[1]").put("b", "v[4]").put("c", "v[3]"));

        query = new MatchStepNew<>(g.V(), "a",
                g.of().as("a").out("knows").as("b"),
                g.of().as("b").out("created").as("c"));
        iter = g.V();
        assertResults(query.solve(iter),
                new Bindings<>().put("a", "v[1]").put("b", "v[4]").put("c", "v[3]"),
                new Bindings<>().put("a", "v[1]").put("b", "v[4]").put("c", "v[5]"));

        query = new MatchStepNew<>(g.V(), "d",
                g.of().as("d").in("knows").as("a"),
                g.of().as("d").has("name", "vadas"),
                g.of().as("a").out("knows").as("b"),
                g.of().as("b").out("created").as("c"));
        iter = g.V();
        assertResults(query.solve(iter),
                new Bindings<>().put("d", "v[2]").put("a", "v[1]").put("b", "v[4]").put("c", "v[3]"),
                new Bindings<>().put("d", "v[2]").put("a", "v[1]").put("b", "v[4]").put("c", "v[5]"));
    }

    @Test
    public void testDAGPatterns() throws Exception {
        // TODO
    }

    @Test
    public void testCyclicPatterns() throws Exception {
        // TODO
    }

    @Test
    public void testTraversalUpdater() throws Exception {
        Graph g = TinkerFactory.createClassic();

        assertBranchFactor(
                2.0,
                g.of().as("a").out("knows").as("b"),
                new SingleIterator<>(g.v(1)));

        assertBranchFactor(
                0.0,
                g.of().as("a").out("foo").as("b"),
                new SingleIterator<>(g.v(1)));

        assertBranchFactor(
                7.0,
                g.of().as("a").both().both().as("b"),
                new SingleIterator<>(g.v(1)));

        assertBranchFactor(
                0.5,
                g.of().as("a").outV().has("name", "marko").as("b"),
                g.E());
    }

    private void assertBranchFactor(final double branchFactor,
                                    final Traversal t,
                                    final Iterator inputs) {
        MatchStepNew.TraversalWrapper w = new MatchStepNew.TraversalWrapper(t, "a", "b");
        MatchStepNew.TraversalUpdater updater = new MatchStepNew.TraversalUpdater<>(w, inputs);
        while (updater.hasNext()) {
            updater.next();
        }
        assertEquals(branchFactor, w.findBranchFactor(), 0);
    }

    private class Bindings<T> implements Comparable<Bindings<T>> {
        private final SortedMap<String, T> map = new TreeMap<>();

        public Bindings<T> put(final String name, final T value) {
            map.put(name, value);
            return this;
        }

        public int compareTo(Bindings<T> other) {
            int cmp = ((Integer) map.size()).compareTo(other.map.size());
            if (0 != cmp) return cmp;

            Iterator<Map.Entry<String, T>> i1 = map.entrySet().iterator();
            Iterator<Map.Entry<String, T>> i2 = other.map.entrySet().iterator();
            while (i1.hasNext()) {
                Map.Entry<String, T> e1 = i1.next();
                Map.Entry<String, T> e2 = i2.next();

                cmp = e1.getKey().compareTo(e1.getKey());
                if (0 != cmp) return cmp;

                cmp = e1.getValue().toString().compareTo(e2.getValue().toString());
                if (0 != cmp) return cmp;
            }

            return 0;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, T> entry : map.entrySet()) {
                if (first) first = false;
                else sb.append(", ");
                sb.append(entry.getKey()).append(":").append(entry.getValue());
            }
            sb.append("}");
            return sb.toString();
        }
    }

    private <T> void assertResults(final MatchStepNew.Enumerator<T> actual,
                                   final Bindings<T>... expected) {
        List<Bindings<T>> actualList = new LinkedList<>();
        int i = 0;
        actualList.add(new Bindings<>());
        while (actual.visitSolution(i++, (name, value) -> {
            actualList.get(actualList.size() - 1).put(name, value);
            return true;
        })) {
            actualList.add(new Bindings<>());
        }
        actualList.remove(actualList.size() - 1);

        List<Bindings<T>> expectedList = new LinkedList<>();
        Collections.addAll(expectedList, expected);

        if (expectedList.size() > actualList.size()) {
            fail("" + (expectedList.size() - actualList.size()) + " expected results not found, including " + expectedList.get(actualList.size()));
        } else if (actualList.size() > expectedList.size()) {
            fail("" + (actualList.size() - expectedList.size()) + " unexpected results, including " + actualList.get(expectedList.size()));
        }

        Collections.sort(actualList);
        Collections.sort(expectedList);

        for (int j = 0; j < actualList.size(); j++) {
            Bindings<T> a = actualList.get(j);
            Bindings<T> e = expectedList.get(j);

            if (0 != a.compareTo(e)) {
                fail("unexpected result(s), including " + a);
            }
        }
    }

    @Test
    public void forJosh() {

        Graph g = TinkerFactory.createClassic();
        GraphTraversal t;

        //////////

        t = g.V().match("a", "c",
                g.of().as("a").out("created").as("b"),
                g.of().as("b").has("name", "lop"),
                g.of().as("b").in("created").as("c"),
                //g.of().as("a").in("knows").as("c"),
                g.of().as("c").has("age", 29))
                .select(As.of("a", "c"), v -> ((Vertex) v).value("name"));

        assertOutputs(t, "[marko, marko]", "[josh, marko]", "[peter, marko]");

        t.forEach(System.out::println);   // TODO: wouldn't it be nice if GraphTraversal iterators were idempotent?

        //////////

        t = g.V().match("a", "c",
                //g.of().as("a").out("created", "knows").as("b"),
                g.of().as("a").out("created").as("b"),
                g.of().as("a").out("knows").as("b"),
                g.of().as("b").identity().as("c"))
                .value("name").path();

        assertOutputs(t, "[v[1], v[3], lop]",
                "[v[1], v[2], vadas]",
                "[v[1], v[4], josh]",
                "[v[4], v[5], ripple]",
                "[v[4], v[3], lop]",
                "[v[6], v[3], lop]");

        //////////

        t = g.V().match("a", "d",
                g.of().as("a").out("created").as("c"),
                g.of().as("a").has("name", "josh"),
                g.of().as("b").out("created").as("c"),
                // ??? a != b
                g.of().as("c").identity().as("d"))
                .value("name").path();

        assertOutputs(t, "[v[4], v[5], ripple]",
                "[v[4], v[3], lop]");

        //////////

        t = g.V().match("a", "b",
                g.of().as("a").out("knows").has("name", "josh"),
                g.of().as("a").out("created").has("name", "lop"),
                g.of().as("a").out("created").as("b"),
                g.of().as("b").has("lang", "java"),
                g.of().as("b").in("created").has("name", "peter"))
                .value("name").path();

        assertOutputs(t, "[v[1], v[3], lop]");
    }

    private void assertOutputs(final GraphTraversal t,
                               final String... resultsToString) {
        Set<String> expected = new HashSet<>();
        Collections.addAll(expected, resultsToString);
        Set<String> actual = new HashSet<>();
        t.forEach(o -> {
            actual.add(o.toString());
        });

        for (String s : expected) {
            if (!actual.contains(s)) {
                fail("expected value not found: " + s);
            }
        }

        for (String s : actual) {
            if (!expected.contains(s)) {
                fail("unexpected value: " + s);
            }
        }
    }
}
