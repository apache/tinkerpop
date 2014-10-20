package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class UntilTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_v1_untilXa_loops_gt_1X_out_asXaX_valueXnameX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_v1_untilXa_1X_out_asXaX_valueXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Long> get_g_V_untilXa_loops_gt_1X_out_asXaX_count();

    public abstract Traversal<Vertex, Long> get_g_V_untilXa_1X_out_asXaX_count();

    public abstract Traversal<Vertex, String> get_g_V_untilXa_1_trueX_out_asXaX_valueXnameX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_untilXa_loops_gt_1X_out_asXaX_valueXnameX() {
        final List<Traversal<Vertex, String>> traversals = Arrays.asList(
                get_g_v1_untilXa_loops_gt_1X_out_asXaX_valueXnameX(convertToVertexId("marko")),
                get_g_v1_untilXa_1X_out_asXaX_valueXnameX(convertToVertexId("marko")));
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList("lop", "ripple"), traversal);
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_untilXa_loops_gt_1X_out_asXaX_count() {
        final List<Traversal<Vertex, Long>> traversals = Arrays.asList(
                get_g_V_untilXa_loops_gt_1X_out_asXaX_count(),
                get_g_V_untilXa_1X_out_asXaX_count());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList(2l), traversal);
        });
    }

    /*@Test
    @LoadGraphWith(MODERN)
    public void g_V_untilXa_1_trueX_out_asXaX_valueXnameX() {
        final List<Traversal<Vertex, String>> traversals = Arrays.asList(get_g_V_untilXa_1_trueX_out_asXaX_valueXnameX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(new HashMap<String, Long>() {{
                put("ripple", 2l);
                put("vadas", 1l);
                put("josh", 1l);
                put("lop", 4l);
            }}, traversal);
        });
    }*/

    public static class StandardTest extends UntilTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex, String> get_g_v1_untilXa_loops_gt_1X_out_asXaX_valueXnameX(final Object v1Id) {
            return g.v(v1Id).until("a", v -> v.loops() > 1).out().as("a").value("name");
        }

        public Traversal<Vertex, String> get_g_v1_untilXa_1X_out_asXaX_valueXnameX(final Object v1Id) {
            return g.v(v1Id).until("a", 1).out().as("a").value("name");
        }

        public Traversal<Vertex, Long> get_g_V_untilXa_loops_gt_1X_out_asXaX_count() {
            return g.V().until("a", v -> v.loops() > 1).out().as("a").count();
        }

        public Traversal<Vertex, Long> get_g_V_untilXa_1X_out_asXaX_count() {
            return g.V().until("a", 1).out().as("a").count();
        }

        public Traversal<Vertex, String> get_g_V_untilXa_1_trueX_out_asXaX_valueXnameX() {
            return g.V().until("a", 1, v -> true).out().as("a").value("name");
        }
    }

    public static class ComputerTest extends UntilTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex, String> get_g_v1_untilXa_loops_gt_1X_out_asXaX_valueXnameX(final Object v1Id) {
            return g.v(v1Id).until("a", v -> v.loops() > 1).out().as("a").<String>value("name").submit(g.compute());
        }

        public Traversal<Vertex, String> get_g_v1_untilXa_1X_out_asXaX_valueXnameX(final Object v1Id) {
            return g.v(v1Id).until("a", 1).out().as("a").<String>value("name").submit(g.compute());
        }

        public Traversal<Vertex, Long> get_g_V_untilXa_loops_gt_1X_out_asXaX_count() {
            return g.V().until("a", v -> v.loops() > 1).out().as("a").count().submit(g.compute());
        }

        public Traversal<Vertex, Long> get_g_V_untilXa_1X_out_asXaX_count() {
            return g.V().until("a", 1).out().as("a").count().submit(g.compute());
        }

        public Traversal<Vertex, String> get_g_V_untilXa_1_trueX_out_asXaX_valueXnameX() {
            return g.V().until("a", 1, v -> true).out().as("a").<String>value("name").submit(g.compute());
        }
    }
}
