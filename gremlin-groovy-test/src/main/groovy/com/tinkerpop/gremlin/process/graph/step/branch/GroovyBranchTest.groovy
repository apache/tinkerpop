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
        public Traversal<Vertex, String> get_g_V_branch_byXsoftware__a__X_byXbX_asXaX_lang_branch_byXcX_asXbX_name_asXcX() {
            g.V.branch.by { it.label() == 'software' ? ['a'] : [] }.by {['b']}.as('a').lang.branch.by { ['c'] }.as('b').name.as('c')
        }
    }

    public static class ComputerTest extends BranchTest {

        @Override
        public Traversal<Vertex, String> get_g_V_branch_byXsoftware__a__X_byXbX_asXaX_lang_branch_byXcX_asXbX_name_asXcX() {
            ComputerTestHelper.compute("g.V.branch.by{it.label() == 'software' ? ['a'] : []}.by{['b']}.as('a').lang.branch.by{['c']}.as('b').name.as('c')", g);
        }
    }
}
