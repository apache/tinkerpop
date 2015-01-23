package com.tinkerpop.gremlin.process.graph.step.branch

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyBranchTest {

    public static class StandardTest extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_forkXa__ageX_forkXb__langX_forkXb__nameX() {
            g.V.branch { it.label() == 'person' ? 'a' : 'b' }
                    .fork('a', __.age)
                    .fork('b', __.lang)
                    .fork('b', __.values('name'))
        }
    }

    public static class ComputerTest extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_forkXa__ageX_forkXb__langX_forkXb__nameX() {
            ComputerTestHelper.compute("g.V.branch { it.label() == 'person' ? 'a' : 'b' }.fork('a', __.age).fork('b', __.lang).fork('b',__.values('name'))", g);
        }
    }
}
