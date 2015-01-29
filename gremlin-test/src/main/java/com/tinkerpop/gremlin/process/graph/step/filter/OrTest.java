package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.__.has;
import static com.tinkerpop.gremlin.process.graph.__.outE;
import static com.tinkerpop.gremlin.structure.Compare.gt;
import static com.tinkerpop.gremlin.structure.Compare.gte;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class OrTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_orXhasXage_gt_27X__outE_count_gt_2X_name();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_orXhasXage_gt_27X__outE_count_gt_2X_name() {
        final Traversal<Vertex, String> traversal = get_g_V_orXhasXage_gt_27X__outE_count_gt_2X_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh", "peter"), traversal);
    }

    public static class StandardTest extends OrTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orXhasXage_gt_27X__outE_count_gt_2X_name() {
            return g.V().or(has("age", gt, 27), outE().count().is(gte, 2l)).values("name");
        }
    }

    public static class ComputerTest extends OrTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orXhasXage_gt_27X__outE_count_gt_2X_name() {
            return g.V().or(has("age", gt, 27), outE().count().is(gte, 2l)).<String>values("name").submit(g.compute());
        }
    }
}
