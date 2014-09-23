package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class UntilTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_v1_untilXa_loops_gt_1X_out_asXaX_valueXnameX(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_untilXa_loops_gt_1X_out_asXaX_valueXnameX() {
        Traversal<Vertex, String> traversal = get_g_v1_untilXa_loops_gt_1X_out_asXaX_valueXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("lop", "ripple"), traversal);
    }

    public static class JavaUntilTest extends UntilTest {
        public JavaUntilTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex, String> get_g_v1_untilXa_loops_gt_1X_out_asXaX_valueXnameX(final Object v1Id) {
            return g.v(v1Id).until("a", v -> v.getLoops() > 1).out().as("a").value("name");
        }
    }

    public static class JavaComputerUntilTest extends UntilTest {
        public JavaComputerUntilTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex, String> get_g_v1_untilXa_loops_gt_1X_out_asXaX_valueXnameX(final Object v1Id) {
            return g.v(v1Id).until("a", v -> v.getLoops() > 1).out().as("a").<String>value("name").submit(g.compute());
        }
    }
}
