package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MaxTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Integer> get_g_V_age_max();

    public abstract Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_max();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_max() {
        final List<Traversal<Vertex, Integer>> traversals = Arrays.asList(
                get_g_V_age_max(),
                get_g_V_repeatXbothX_timesX5X_age_max());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList(35), traversal);
        });
    }

    public static class StandardTest extends MaxTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_max() {
            return g.V().values("age").max();
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_max() {
            return g.V().repeat(__.both()).times(5).values("age").max();
        }

    }

    public static class ComputerTest extends MaxTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_max() {
            return g.V().values("age").<Integer>max().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_max() {
            return g.V().repeat(__.both()).times(5).values("age").<Integer>max().submit(g.compute());
        }
    }
}