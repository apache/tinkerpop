package com.tinkerpop.gremlin.process.graph.traversal.filter

import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.OrTest
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.traversal.__.has
import static com.tinkerpop.gremlin.process.graph.traversal.__.outE
import static com.tinkerpop.gremlin.structure.Compare.gt
import static com.tinkerpop.gremlin.structure.Compare.gte

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyOrTest {

    public static class StandardTest extends OrTest {

        @Override
        public Traversal<Vertex, String> get_g_V_orXhasXage_gt_27X__outE_count_gt_2X_name() {
            g.V.or(has('age', gt, 27), outE().count.is(gte, 2l)).name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name() {
            g.V.or(outE('knows'), has(T.label, 'software').or.has('age', gte, 35)).name
        }
    }

    public static class ComputerTest extends OrTest {

        @Override
        public Traversal<Vertex, String> get_g_V_orXhasXage_gt_27X__outE_count_gt_2X_name() {
            ComputerTestHelper.compute("g.V.or(has('age', gt, 27), outE().count.is(gte, 2l)).name", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name() {
            ComputerTestHelper.compute("g.V.or(outE('knows'), has(T.label, 'software').or.has('age', gte, 35)).name", g)
        }
    }
}
