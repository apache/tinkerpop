package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class BranchTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_forkXa__ageX_forkXb__langX_forkXb__nameX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_branch_byXsoftware__a_bX_asXaX_lang_branchXcX_asXbX_name_asXcX() {
        final Traversal<Vertex, Object> traversal = get_g_V_branchXlabel_eq_person__a_bX_forkXa__ageX_forkXb__langX_forkXb__nameX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("java", "java", "lop", "ripple", 29, 27, 32, 35), traversal);
    }


    public static class StandardTest extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_forkXa__ageX_forkXb__langX_forkXb__nameX() {
            return g.V().branch(v -> v.get().label().equals("person") ? "a" : "b")
                    .fork("a", __.values("age"))
                    .fork("b", __.values("lang"))
                    .fork("b", __.values("name"));
        }
    }

    public static class ComputerTest extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_forkXa__ageX_forkXb__langX_forkXb__nameX() {
            return g.V().branch(v -> v.get().label().equals("person") ? "a" : "b")
                    .fork("a", __.values("age"))
                    .fork("b", __.values("lang"))
                    .fork("b", __.values("name")).submit(g.compute());
        }
    }
}