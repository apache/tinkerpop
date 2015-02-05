package com.tinkerpop.gremlin.process.graph.traversal.step.branch

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.BranchTest
import com.tinkerpop.gremlin.structure.Vertex

import com.tinkerpop.gremlin.process.graph.traversal.__
/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyBranchTest {

    public static class StandardTest extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
            g.V.branch { it.label() == 'person' ? 'a' : 'b' }
                    .option('a', __.age)
                    .option('b', __.lang)
                    .option('b', __.name)
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabelX_optionXperson__ageX_optionXsoftware__langX_optionXsoftware__nameX() {
            g.V.branch(__.label)
                    .option('person', __.age)
                    .option('software', __.lang)
                    .option('software', __.name);
        }
    }

    public static class ComputerTest extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
            ComputerTestHelper.compute("g.V.branch { it.label() == 'person' ? 'a' : 'b' }.option('a', __.age).option('b', __.lang).option('b',__.name)", g);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabelX_optionXperson__ageX_optionXsoftware__langX_optionXsoftware__nameX() {
            ComputerTestHelper.compute("""
            g.V.branch(__.label)
                    .option('person', __.age)
                    .option('software', __.lang)
                    .option('software', __.name)
            """, g)
        }
    }
}
