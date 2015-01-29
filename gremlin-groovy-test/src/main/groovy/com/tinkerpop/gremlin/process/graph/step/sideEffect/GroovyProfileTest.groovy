package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.process.util.StandardTraversalMetrics
import com.tinkerpop.gremlin.structure.Vertex
import com.tinkerpop.gremlin.process.graph.__
/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyProfileTest {

    public static class StandardTest extends ProfileTest {

        @Override
        Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            g.V.out.out.profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile() {
            g.V.repeat(__.both()).times(3).profile();
        }

    }

    public static class ComputerTest extends ProfileTest {

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            ComputerTestHelper.compute("g.V.out.out.profile()", g);
        }


        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile() {
           ComputerTestHelper.compute("g.V.repeat(__.both()).times(3).profile()", g);
        }
    }
}

