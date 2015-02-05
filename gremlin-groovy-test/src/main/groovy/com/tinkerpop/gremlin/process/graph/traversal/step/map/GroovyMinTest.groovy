package com.tinkerpop.gremlin.process.graph.traversal.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.traversal.__
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MinTest
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyMinTest {

    public static class StandardTest extends MinTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_min() {
            g.V.age.min
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min() {
            g.V.repeat(__.both).times(5).age.min
        }

    }

    public static class ComputerTest extends MinTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_min() {
            ComputerTestHelper.compute("g.V.age.min", g)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min() {
            ComputerTestHelper.compute("g.V.repeat(__.both).times(5).age.min", g)
        }
    }
}
