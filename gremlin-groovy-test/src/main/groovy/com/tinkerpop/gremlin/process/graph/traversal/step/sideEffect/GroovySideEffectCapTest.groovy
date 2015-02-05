package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SideEffectCapTest
import com.tinkerpop.gremlin.structure.Vertex
import com.tinkerpop.gremlin.process.graph.traversal.__
/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySideEffectCapTest {

    public static class StandardTest extends SideEffectCapTest {
        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX() {
            g.V.has('age').groupCount('a').by('name').out.cap('a')
        }

        @Override
        public Traversal<Vertex, Map<String, Map<Object, Long>>> get_g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX() {
            g.V.choose(__.has(T.label, 'person'),
                    __.age.groupCount('a'),
                    __.values("name").groupCount('b')).cap('a', 'b')
        }
    }

    public static class ComputerTest extends SideEffectCapTest {
        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX() {
            ComputerTestHelper.compute("g.V.has('age').groupCount('a').by('name').out.cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Map<Object, Long>>> get_g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX() {
            ComputerTestHelper.compute("""
            g.V.choose(__.has(T.label, 'person'),
                    __.age.groupCount('a'),
                    __.values("name").groupCount('b')).cap('a', 'b')
            """, g)
        }
    }
}
