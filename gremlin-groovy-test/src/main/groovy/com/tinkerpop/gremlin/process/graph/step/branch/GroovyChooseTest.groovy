package com.tinkerpop.gremlin.process.graph.step.branch

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class GroovyChooseTest {

    public static class StandardTest extends ChooseTest {

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name() {
            g.V.choose({ it.name.length() == 5 },
                    __.out(),
                    __.in()).name;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_chooseX0X_forkX0__outX_name(Object v1Id) {
            g.V(v1Id).choose { 0 }.fork(0, __.out.name)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXlabel_personX_chooseXname_lengthX_forkX5__inX_forkX4__outX_forkX3__bothX_name() {
            g.V.has(T.label, 'person').choose { it.name.length() }.fork(5, __.in).fork(4, __.out).fork(3, __.both).name
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_count_nextX_forkX2L__nameX_forkX3L__valueMapX() {
            g.V.choose { it.out.count().next() }.fork(2L, __.values('name')).fork(3L, __.valueMap())
        }
    }

    public static class ComputerTest extends ChooseTest {

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name() {
            ComputerTestHelper.compute("""g.V.choose({ it.name.length() == 5 },
                    __.out(),
                    __.in).name""", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_chooseX0X_forkX0__outX_name(Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).choose { 0 }.fork(0, __.out.name)", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXlabel_personX_chooseXname_lengthX_forkX5__inX_forkX4__outX_forkX3__bothX_name() {
            ComputerTestHelper.compute("g.V.has(T.label,'person').choose { it.name.length() }.fork(5, __.in).fork(4, __.out).fork(3, __.both).name", g);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_count_nextX_forkX2L__nameX_forkX3L__valueMapX() {
            ComputerTestHelper.compute("g.V.choose { it.out.count().next() }.fork(2L, __.values('name')).fork(3L, __.valueMap())", g);
        }
    }
}
