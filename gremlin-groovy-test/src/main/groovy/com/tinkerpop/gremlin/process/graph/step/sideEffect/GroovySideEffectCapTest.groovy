package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySideEffectCapTest {

    public static class StandardTest extends SideEffectCapTest {
        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX() {
            g.V.has('age').groupCount('a').by('name').out.cap('a')
        }
    }

    public static class ComputerTest extends SideEffectCapTest {
        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX() {
            ComputerTestHelper.compute("g.V.has('age').groupCount('a').by('name').out.cap('a')", g)
        }
    }
}
