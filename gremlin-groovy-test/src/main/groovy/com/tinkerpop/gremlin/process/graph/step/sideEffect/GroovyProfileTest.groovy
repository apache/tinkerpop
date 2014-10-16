package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.process.util.TraversalMetrics
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyProfileTest {

    public static class StandardTest extends ProfileTest {

        @Override
        Traversal<Vertex, TraversalMetrics> get_g_V_out_out_profile() {
            g.V.out.out.profile();
        }

    }

    public static class ComputerTest extends ProfileTest {

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_out_out_profile() {
            ComputerTestHelper.compute("g.V.out.out.profile()", g);
        }

    }
}

