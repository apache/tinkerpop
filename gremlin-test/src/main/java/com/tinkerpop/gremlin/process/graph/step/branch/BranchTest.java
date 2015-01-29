package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.__.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class BranchTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX();

    public abstract Traversal<Vertex, Object> get_g_V_branchXlabelX_optionXperson__ageX_optionXsoftware__langX_optionXsoftware__nameX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
        final List<Traversal<Vertex, Object>> traversals = Arrays.asList(
                get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX(),
                get_g_V_branchXlabelX_optionXperson__ageX_optionXsoftware__langX_optionXsoftware__nameX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList("java", "java", "lop", "ripple", 29, 27, 32, 35), traversal);
        });
    }


    public static class StandardTest extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
            return g.V().branch(v -> v.get().label().equals("person") ? "a" : "b")
                    .option("a", values("age"))
                    .option("b", values("lang"))
                    .option("b", values("name"));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabelX_optionXperson__ageX_optionXsoftware__langX_optionXsoftware__nameX() {
            return g.V().branch(label())
                    .option("person", values("age"))
                    .option("software", values("lang"))
                    .option("software", values("name"));
        }
    }

    public static class ComputerTest extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
            return g.V().branch(v -> v.get().label().equals("person") ? "a" : "b")
                    .option("a", values("age"))
                    .option("b", values("lang"))
                    .option("b", values("name")).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabelX_optionXperson__ageX_optionXsoftware__langX_optionXsoftware__nameX() {
            return g.V().branch(label())
                    .option("person", values("age"))
                    .option("software", values("lang"))
                    .option("software", values("name")).submit(g.compute());
        }
    }
}