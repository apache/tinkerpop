package com.tinkerpop.gremlin.process.graph.step.branch

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyBranchTest {

    public static class StandardTest extends BranchTest {

        @Override
        public Traversal<Vertex, String> get_g_V_branch_byXsoftware__a_bX_asXaX_lang_branchXcX_asXbX_name_asXcX() {
            g.V.branch { it.label() == 'software' ? ['a', 'b'] : ['b'] }.as('a').lang.branch {
                ['c']
            }.as('b').name.as('c')
        }
    }

    public static class ComputerTest extends BranchTest {

        @Override
        public Traversal<Vertex, String> get_g_V_branch_byXsoftware__a_bX_asXaX_lang_branchXcX_asXbX_name_asXcX() {
            ComputerTestHelper.compute("g.V.branch{it.label() == 'software' ? ['a','b'] : ['b']}.as('a').lang.branch{['c']}.as('b').name.as('c')", g);
        }
    }
}
