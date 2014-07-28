package com.tinkerpop.gremlin.tinkergraph.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.match.CrossJoinEnumerator;
import com.tinkerpop.gremlin.process.graph.step.map.match.Enumerator;
import com.tinkerpop.gremlin.process.graph.step.map.match.InnerJoinEnumerator;
import com.tinkerpop.gremlin.process.graph.step.map.match.IteratorEnumerator;
import com.tinkerpop.gremlin.process.graph.step.map.match.MatchStepNew;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchStepTest {

    @Test
    public void testOutputs() throws Exception {
        MatchStepNew<Object, Object> query;
        Iterator iter;
        Graph g = TinkerFactory.createClassic();

        iter = g.V();
        query = new MatchStepNew<>(g.V(), "a",
                g.of().as("a").out("knows").as("b"),
                g.of().as("a").out("created").as("c"));

        GraphTraversal t = g.V().match("a", g.of().as("a").out("knows").as("b"),
                g.of().as("a").out("created").as("c"));
        while (t.hasNext()) {
            System.out.println("solution: " + t.next());
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
        assertResults(query.solveFor(iter),
                new Bindings<>().put("a", "v[1]").put("b", "v[2]").put("c", "v[3]"),
                new Bindings<>().put("a", "v[1]").put("b", "v[4]").put("c", "v[3]"));

        query = new MatchStepNew<>(g.V(), "a",
                g.of().as("a").out("knows").as("b"),
                g.of().as("b").out("created").as("c"));
        iter = g.V();
        assertResults(query.solveFor(iter),
                new Bindings<>().put("a", "v[1]").put("b", "v[4]").put("c", "v[3]"),
                new Bindings<>().put("a", "v[1]").put("b", "v[4]").put("c", "v[5]"));

        query = new MatchStepNew<>(g.V(), "d",
                g.of().as("d").in("knows").as("a"),
                g.of().as("d").has("name", "vadas"),
                g.of().as("a").out("knows").as("b"),
                g.of().as("b").out("created").as("c"));
        iter = g.V();
        assertResults(query.solveFor(iter),
                new Bindings<>().put("d", "v[2]").put("a", "v[1]").put("b", "v[4]").put("c", "v[3]"),
                new Bindings<>().put("d", "v[2]").put("a", "v[1]").put("b", "v[4]").put("c", "v[5]"));
    }

    @Test
    public void testDAGPatterns() throws Exception {
        MatchStepNew<Object, Object> query;
        Iterator iter;
        Graph g = TinkerFactory.createModern();

        iter = g.V();
        query = new MatchStepNew<>(g.V(), "a",
                g.of().as("a").out("uses").as("b"),
                g.of().as("b").out("dependsOn").as("c"),
                g.of().as("a").out("created").as("c"));

        assertResults(query.solveFor(iter),
                new Bindings<>().put("a", "v[1]").put("b", "v[10]").put("c", "v[11]"));
    }

    // TODO
    /*
    @Test
    public void testCyclicPatterns() throws Exception {
        MatchStepNew<Object, Object> query;
        Iterator iter;
        Graph g = TinkerFactory.createModern();

        iter = g.V();
        query = new MatchStepNew<>(g.V(), "a",
                g.of().as("a").out("uses").as("b"),
                g.of().as("b").out("dependsOn").as("c"),
                g.of().as("c").in("created").as("a"));

        assertResults(query.solve(iter),
                new Bindings<>().put("a", "v[1]").put("b", "v[10]").put("c", "v[11]"));
    }
    */

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

    @Test
    public void testOptimization() throws Exception {
        MatchStepNew<Object, Object> query;
        Iterator iter;
        Graph g = TinkerFactory.createClassic();

        query = new MatchStepNew<>(g.V(), "d",
                g.of().as("d").in("knows").as("a"),
                g.of().as("d").has("name", "vadas"),
                g.of().as("a").out("knows").as("b"),
                g.of().as("b").out("created").as("c"));
        iter = g.V();
        query.optimize();
        System.out.println(query.summarize());
        // c costs nothing (no outgoing traversals)
        assertEquals(0.0, query.findCost("c"), 0);
        // b-created->c has a cost equal to its branch factor, 1.0
        // b has only one outgoing traversal, b-created->c, so its total cost is 1.0
        assertEquals(1.0, query.findCost("b"), 0);
        // the cost of a-knows->b is its branch factor (1.0) plus the branch factor times the cost of b-created->c (1.0), so 2.0
        // a has only one outgoing traversal, a-knows->b, so its total cost is 2.0
        assertEquals(2.0, query.findCost("a"), 0);
        // the cost of d<-knows-a is its branch factor (1.0) plus the branch factor times the cost of a-knows->b (2.0), so 3.0
        // the cost of d->has(name,vadas) is its branch factor (1.0)
        // the total cost of d is the cost of its first traversal times the branch factor of the first times the cost of the second,
        //     or 3.0 + 1.0*1.0 = 4.0
        assertEquals(4.0, query.findCost("d"), 0);

        // apply the query to the graph, gathering non-trivial branch factors
        assertResults(query.solveFor(iter),
                new Bindings<>().put("d", "v[2]").put("a", "v[1]").put("b", "v[4]").put("c", "v[3]"),
                new Bindings<>().put("d", "v[2]").put("a", "v[1]").put("b", "v[4]").put("c", "v[5]"));
        query.optimize();
        System.out.println(query.summarize());
        // c still costs nothing (no outgoing traversals)
        assertEquals(0.0, query.findCost("c"), 0);
        // b-created->c still has a branch factor of 1.0, as we have put two items in (josh and vadas) and gotten two out (lop and ripple)
        // b has only one outgoing traversal, b-created->c, so its total cost is 1.0
        assertEquals(1.0, query.findCost("b"), 0);
        // a-knows->b now has a branch factor of 2.0 -- we put in marko and got out josh and vadas
        // the cost of a-knows->b is its branch factor (2.0) plus the branch factor times the cost of b-created->c (1.0), so 4.0
        // a has only one outgoing traversal, a-knows->b, so its total cost is 4.0
        assertEquals(4.0, query.findCost("a"), 0);
        // d<-knows-a has a branch factor of 1/6 -- we put in all six vertices and got out marko
        //     we get out marko only once, because d-name->"vadas" is tried first and rules out all but one "d"
        // the cost of d<-knows-a is its branch factor (1/6) plus the branch factor times the cost of a-knows->b (4.0), so 5/6
        // since we optimized to put the has step first (it immediately eliminates most vertices),
        //     the cost of d->has(name,vadas) is 1/6 -- we put in all six vertices and got out one
        // the total cost of d is the cost of its first traversal times the branch factor of the first times the cost of the second,
        //     or 1/6 + 1/6*5/6 = 11/36
        assertEquals(11 / 36.0, query.findCost("d"), 0.001);
    }

    @Test
    public void testPerformance() throws Exception {
        Graph g = TinkerGraph.open();
        try (InputStream in = Graph.class.getResourceAsStream("util/io/graphml/grateful-dead.xml")) {
            GraphMLReader r = GraphMLReader.create().build();
            r.readGraph(in, g);
        }

        long startTime, endTime;
        startTime = System.currentTimeMillis();
        MatchStepNew<Object, Object> query = new MatchStepNew<>(g.V().has("name", "Garcia"), "garcia",
                g.of().as("garcia").in("sung_by").as("song"),
                g.of().as("song").out("written_by", "writer"));
        Enumerator<Object> solutions = query.solveFor(g.V().has("name", "Garcia"));
        exhaust(solutions);
        endTime = System.currentTimeMillis();
        assertEquals(147, solutions.size());
        System.out.println("finished in " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        long count = g.V().has("name", "Garcia").as("garcia").in("sung_by").as("song").out("written_by").as("writer").count().next();
        endTime = System.currentTimeMillis();
        assertEquals(147, count);
        System.out.println("finished in " + (endTime - startTime) + "ms");
    }

    @Test
    public void testCrossJoin() throws Exception {
        String[] a1 = new String[]{"a", "b", "c"};
        String[] a2 = new String[]{"1", "2", "3", "4"};
        String[] a3 = new String[]{"@", "#"};

        Enumerator<String> e1 = new IteratorEnumerator<>("letter", Arrays.asList(a1).iterator());
        Enumerator<String> e2 = new IteratorEnumerator<>("number", Arrays.asList(a2).iterator());
        Enumerator<String> e3 = new IteratorEnumerator<>("punc", Arrays.asList(a3).iterator());

        Enumerator<String> e1e2 = new CrossJoinEnumerator<>(e1, e2);
        BiConsumer<String, String> visitor = (name, value) -> {
            //System.out.println("\t" + name + ":\t" + value);
        };
        Enumerator<String> e1e2e3 = new CrossJoinEnumerator<>(e1e2, e3);

        int i = 0;
        Enumerator<String> e
                = e1e2e3; //e1e2;
        while (e.visitSolution(i, visitor)) {
            //System.out.println("solution #" + (i + 1) + "^^");
            i++;
        }
        assertEquals(24, i);
    }

    /*
    @Test
    public void testCrossJoinLaziness() throws Exception {
        List<Integer> value = new LinkedList<>();
        for (int j = 0; j < 1000; j++) {
            value.add(j);
        }

        int base = 3;
        List<Enumerator<Integer>> enums = new LinkedList<>();
        for (int k = 0; k < base; k++) {
            List<Integer> lNew = new LinkedList<>();
            lNew.addAll(value);
            Enumerator<Integer> ek = new IteratorEnumerator<>("" + (char) ('a' + k), lNew.iterator());
            enums.add(ek);
        }

        Enumerator<Integer> e = new NewCrossJoinEnumerator<>(enums);

        // we now have an enumerator of 5^3^10 elements
        EnumeratorIterator<Integer> iter = new EnumeratorIterator<>(e);

        int count = 0;
        // each binding set is unique
        Set<String> values = new HashSet<>();
        String s;
        s = iter.next().toString();
        values.add(s);
        assertEquals(++count, values.size());
        // begin at the head of all iterators
        assertEquals("{a=0, b=0, c=0, d=0, e=0, f=0, g=0, h=0, i=0, j=0}", s);
        int lim0 = (int) Math.pow(1, base);
        // first 2^10 results are binary (0's and 1's)
        int lim1 = (int) Math.pow(2, base);
        for (int i = lim0; i < lim1; i++) {
            s = iter.next().toString();
            System.out.println("" + i + ": " + count + ": " + s); System.out.flush();
            assertTrue(s.contains("1"));
            assertFalse(s.contains("2"));
            values.add(s);
            assertEquals(++count, values.size());
        }
        int lim2 = (int) Math.pow(3, base);
        for (int i = lim1; i < lim2; i++) {
            s = iter.next().toString();
            System.out.println("" + i + ": " + count + ": " + s); System.out.flush();

            if (!s.contains("2")) {
                findMissing(null, 0, base, "abcdefghij".getBytes(), values);
            }


            assertTrue(s.contains("2"));
            assertFalse(s.contains("3"));
            values.add(s);
            assertEquals(++count, values.size());
        }
    }
    */

    @Test
    public void testInnerJoin() throws Exception {
        String[] a1 = new String[]{"a", "b", "c"};
        String[] a2 = new String[]{"1", "2", "3", "4"};
        String[] a3 = new String[]{"2", "4", "6", "8", "10"};

        Enumerator<String> e1 = new IteratorEnumerator<>("letter", Arrays.asList(a1).iterator());
        Enumerator<String> e2 = new IteratorEnumerator<>("number", Arrays.asList(a2).iterator());
        Enumerator<String> e3 = new IteratorEnumerator<>("number", Arrays.asList(a3).iterator());

        Enumerator<String> e4 = new CrossJoinEnumerator<>(e1, e3);
        Enumerator<String> e5 = new CrossJoinEnumerator<>(e2, e4);

        // without AND semantics, we have all 60 combinations, including two "number" bindings per solution
        exhaust(e5);
        assertEquals(60, e5.size());

        Enumerator<String> join = new InnerJoinEnumerator<>(e5, new HashSet<String>() {{
            add("number");
        }});
        exhaust(join);
        assertEquals(6, join.size());

        assertResults(join,
                new Bindings<String>().put("letter", "a").put("number", "2"),
                new Bindings<String>().put("letter", "a").put("number", "4"),
                new Bindings<String>().put("letter", "b").put("number", "2"),
                new Bindings<String>().put("letter", "b").put("number", "4"),
                new Bindings<String>().put("letter", "c").put("number", "2"),
                new Bindings<String>().put("letter", "c").put("number", "4"));
    }

    private void findMissing(String s, final int i, final int n, final byte[] names, final Set<String> actual) {
        if (0 == i) {
            s = "{";
        } else {
            s += ", ";
        }

        s += (char) names[i];
        s += "=";
        String tmp = s;
        for (int j = 0; j < 3; j++) {
            s += j;

            if (n - 1 == i) {
                s += "}";
                if (!actual.contains(s)) {
                    fail("not in set: " + s);
                }
            } else {
                findMissing(s, i + 1, n, names, actual);
            }

            s = tmp;
        }
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

        public Bindings() {
        }

        public Bindings(final Map<String, T> map) {
            this.map.putAll(map);
        }

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

    private <T> List<Bindings<T>> toBindings(final Enumerator<T> enumerator) {
        List<Bindings<T>> bindingsList = new LinkedList<>();
        int i = 0;
        bindingsList.add(new Bindings<>());
        while (enumerator.visitSolution(i++, (name, value) -> {
            bindingsList.get(bindingsList.size() - 1).put(name, value);
        })) {
            bindingsList.add(new Bindings<>());
        }
        bindingsList.remove(bindingsList.size() - 1);

        return bindingsList;
    }

    private <T> void assertResults(final Enumerator<T> actual,
                                   final Bindings<T>... expected) {

        List<Bindings<T>> actualList = toBindings(actual);
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

    /*
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
    */

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

    private <T> void exhaust(Enumerator<T> enumerator) {
        BiConsumer<String, T> visitor = (s, t) -> {};
        int i = 0;
        if (!enumerator.isComplete()) {
            while (enumerator.visitSolution(i, visitor)) i++;
        }
    }
}
