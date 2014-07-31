package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.match.Bindings;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.fail;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class MatchTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_a_outXknowsX_b__b_outXcreatedX_c();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_a_out_b();

    @Test
    public void testNothing() {}

    /*
    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_a_out_b() throws Exception {

        Traversal<Vertex, Map<String, Object>> t = get_g_V_a_outXknowsX_b__b_outXcreatedX_c();
        assertResults(t,
                new Bindings<>().put("a", "v[1]").put("b", "v[3]"),
                new Bindings<>().put("a", "v[1]").put("b", "v[2]"),
                new Bindings<>().put("a", "v[1]").put("b", "v[4]"),
                new Bindings<>().put("a", "v[4]").put("b", "v[5]"),
                new Bindings<>().put("a", "v[4]").put("b", "v[3]"),
                new Bindings<>().put("a", "v[6]").put("b", "v[3]"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_a_outXknowsX_b__b_outXcreatedX_c() throws Exception {

        Traversal<Vertex, Map<String, Object>> t = get_g_V_a_outXknowsX_b__b_outXcreatedX_c();
        assertResults(t,
                new Bindings<>().put("a", "v[1]").put("b", "v[4]").put("c", "v[3]"),
                new Bindings<>().put("a", "v[1]").put("b", "v[4]").put("c", "v[5]"));
    }
    */

    public static class JavaMapTest extends MatchTest {
        public JavaMapTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_a_outXknowsX_b__b_outXcreatedX_c() {
            return g.V().match("a",
                    g.of().as("a").out("knows").as("b"),
                    g.of().as("b").out("created").as("c"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_a_out_b() {
            return g.V().match("a", g.of().as("a").out().as("b"));
        }
    }

    private <S, E> void assertResults(final Traversal<S, Map<String, E>> actual,
                                      final Bindings<E>... expected) {

        List<Bindings<E>> actualList = toBindings(actual);
        List<Bindings<E>> expectedList = new LinkedList<>();
        Collections.addAll(expectedList, expected);

        if (expectedList.size() > actualList.size()) {
            fail("" + (expectedList.size() - actualList.size()) + " expected results not found, including " + expectedList.get(actualList.size()));
        } else if (actualList.size() > expectedList.size()) {
            fail("" + (actualList.size() - expectedList.size()) + " unexpected results, including " + actualList.get(expectedList.size()));
        }

        Collections.sort(actualList);
        Collections.sort(expectedList);

        for (int j = 0; j < actualList.size(); j++) {
            Bindings<E> a = actualList.get(j);
            Bindings<E> e = expectedList.get(j);

            if (0 != a.compareTo(e)) {
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
