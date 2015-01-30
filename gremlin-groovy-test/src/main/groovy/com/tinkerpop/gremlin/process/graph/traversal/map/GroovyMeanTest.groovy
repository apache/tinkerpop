package com.tinkerpop.gremlin.process.graph.traversal.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MeanTest
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyMeanTest {

    public static class StandardTest extends MeanTest {

        @Override
        public Traversal<Vertex, Double> get_g_V_age_mean() {
            g.V.age.mean
        }

    }

    public static class ComputerTest extends MeanTest {

        @Override
        public Traversal<Vertex, Double> get_g_V_age_mean() {
            ComputerTestHelper.compute("g.V.age.mean", g)
        }
    }
}