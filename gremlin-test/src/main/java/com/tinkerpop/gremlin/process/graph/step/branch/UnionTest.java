package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashMap;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class UnionTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_unionXout_inX_name();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_unionXout_inX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_unionXout_inX_name();
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

    public static class StandardTest extends UnionTest {

        public Traversal<Vertex, String> get_g_V_unionXout_inX_name() {
            return g.V().union(g.<Vertex>of().out(), g.<Vertex>of().in()).values("name");
        }
    }

    public static class ComputerTest extends UnionTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex, String> get_g_V_unionXout_inX_name() {
            return g.V().union(g.<Vertex>of().out(), g.<Vertex>of().in()).<String>values("name").submit(g.compute());
        }
    }
}
