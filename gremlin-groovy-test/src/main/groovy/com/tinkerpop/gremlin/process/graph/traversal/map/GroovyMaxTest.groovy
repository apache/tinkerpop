package com.tinkerpop.gremlin.process.graph.traversal.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.__
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MaxTest
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyMaxTest {

    public static class StandardTest extends MaxTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_max() {
            g.V.age.max
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_max() {
            g.V.repeat(__.both).times(5).age.max
        }

    }

    public static class ComputerTest extends MaxTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_max() {
            ComputerTestHelper.compute("g.V.age.max", g)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_max() {
            ComputerTestHelper.compute("g.V.repeat(__.both).times(5).age.max", g)
        }
    }
}
