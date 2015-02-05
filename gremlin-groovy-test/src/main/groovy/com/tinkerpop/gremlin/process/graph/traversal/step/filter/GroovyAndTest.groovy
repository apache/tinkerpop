package com.tinkerpop.gremlin.process.graph.traversal.step.filter

import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.AndTest
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.traversal.__.has
import static com.tinkerpop.gremlin.process.graph.traversal.__.outE
import static com.tinkerpop.gremlin.structure.Compare.gt
import static com.tinkerpop.gremlin.structure.Compare.gte

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAndTest {

    public static class StandardTest extends AndTest {

        @Override
        public Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name() {
            g.V.and(has('age', gt, 27), outE().count.is(gte, 2l)).name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name() {
            g.V.and(outE(), has(T.label, 'person').and.has('age', gte, 32)).name
        }
    }

    public static class ComputerTest extends AndTest {

        @Override
        public Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name() {
            ComputerTestHelper.compute("g.V.and(has('age', gt, 27), outE().count.is(gte, 2l)).name", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name() {
            ComputerTestHelper.compute("g.V.and(outE(), has(T.label, 'person').and.has('age', gte, 32)).name", g)
        }
    }
}
