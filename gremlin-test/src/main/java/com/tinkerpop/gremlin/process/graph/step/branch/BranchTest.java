package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class BranchTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_branch_byXsoftware__a_bX_asXaX_lang_branchXcX_asXbX_name_asXcX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_branch_byXsoftware__a_bX_asXaX_lang_branchXcX_asXbX_name_asXcX() {
        final Traversal<Vertex, String> traversal = get_g_V_branch_byXsoftware__a_bX_asXaX_lang_branchXcX_asXbX_name_asXcX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("java", "java", "marko", "vadas", "josh", "lop", "ripple", "peter"), traversal);
    }


    public static class StandardTest extends BranchTest {

        @Override
        public Traversal<Vertex, String> get_g_V_branch_byXsoftware__a_bX_asXaX_lang_branchXcX_asXbX_name_asXcX() {
            return g.V().branch(t -> t.get().label().equals("software") ? Arrays.asList("a", "b") : Collections.singletonList("b")).as("a").values("lang").branch(t -> Collections.singletonList("c")).as("b").<String>values("name").as("c");
        }
    }

    public static class ComputerTest extends BranchTest {

        @Override
        public Traversal<Vertex, String> get_g_V_branch_byXsoftware__a_bX_asXaX_lang_branchXcX_asXbX_name_asXcX() {
            return g.V().branch(t -> t.get().label().equals("software") ? Arrays.asList("a", "b") : Collections.singletonList("b")).as("a").values("lang").branch(t -> Collections.singletonList("c")).as("b").<String>values("name").as("c").submit(g.compute());
        }
    }
}