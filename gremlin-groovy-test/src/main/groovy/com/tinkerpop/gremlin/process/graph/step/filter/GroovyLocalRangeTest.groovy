package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyLocalRangeTest {

    public static class StandardTest extends LocalRangeTest {

        @Override
        public Traversal<Vertex, Edge> get_g_V_outE_localRangeX0_2X() {
            g.V.outE.localRange(0, 2)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_propertiesXlocationX_orderByXvalueX_localRangeX0_2X_value() {
            g.V.properties('location').orderBy(T.value).localRange(0, 2).value
        }
    }

    public static class ComputerTest extends LocalRangeTest {

        @Override
        public Traversal<Vertex, Edge> get_g_V_outE_localRangeX0_2X() {
            ComputerTestHelper.compute("g.V.outE.localRange(0, 2)", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_propertiesXlocationX_orderByXvalueX_localRangeX0_2X_value() {
            g.V.properties('location').orderBy(T.value).localRange(0, 2).value // todo
        }
    }
}
