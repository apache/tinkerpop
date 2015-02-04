package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.traversal.__.*;
import static com.tinkerpop.gremlin.structure.Compare.gt;
import static com.tinkerpop.gremlin.structure.Compare.gte;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AndTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name();

    public abstract Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_andXhasXage_gt_27X__outE_count_gt_2X_name() {
        final Traversal<Vertex, String> traversal = get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_andXout__hasXlabel_personX_and_hasXage_gte_32XX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "peter"), traversal);
    }

    public static class StandardTest extends AndTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name() {
            return g.V().and(has("age", gt, 27), outE().count().is(gte, 2l)).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name() {
            return g.V().and(outE(), has(T.label, "person").and().has("age", Compare.gte, 32)).values("name");
        }
    }

    public static class ComputerTest extends AndTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name() {
            return g.V().and(has("age", gt, 27), outE().count().is(gte, 2l)).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name() {
            return g.V().and(outE(), has(T.label, "person").and().has("age", Compare.gte, 32)).<String>values("name"); // .submit(g.compute());
        }
    }
}
