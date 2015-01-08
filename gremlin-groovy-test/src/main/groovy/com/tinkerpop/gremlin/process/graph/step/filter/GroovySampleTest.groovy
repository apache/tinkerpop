package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySampleTest {

    public static class StandardTest extends SampleTest {

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX1X() {
            g.E.sample(1)
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX2X_byXweightX() {
            g.E.sample(2).by('weight')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_localXoutE_sampleX1X_byXweightXX() {
            g.V.local(__.outE.sample(1).by('weight'))
        }
    }

    public static class ComputerTest extends SampleTest {

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX1X() {
            g.E.sample(1) // TODO: makes no sense when its global
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX2X_byXweightX() {
            g.E.sample(2).by('weight') // TODO: makes no sense when its global
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_localXoutE_sampleX1X_byXweightXX() {
            ComputerTestHelper.compute("g.V.local(__.outE.sample(1).by('weight'))", g)
        }
    }
}
