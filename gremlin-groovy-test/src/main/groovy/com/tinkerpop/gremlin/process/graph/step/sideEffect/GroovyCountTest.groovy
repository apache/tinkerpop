package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyCountTest {

    public static class StandardTest extends CountTest {
        @Override
        public Traversal<Vertex, Long> get_g_V_count() {
            g.V.count()
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_out_count() {
            g.V.out.count
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_both_both_count() {
            g.V.both.both.count()
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count() {
            g.V().repeat(__.out).times(3).count()
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count() {
            g.V.repeat(__.out).times(8).count()
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_filterXfalseX_count() {
            g.V.filter { false }.count
        }
    }

    public static class ComputerTest extends CountTest {
        @Override
        public Traversal<Vertex, Long> get_g_V_count() {
            ComputerTestHelper.compute("g.V.count()", g)
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_out_count() {
            ComputerTestHelper.compute("g.V.out.count", g)
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_both_both_count() {
            ComputerTestHelper.compute("g.V.both.both.count()", g)
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count() {
            ComputerTestHelper.compute("g.V().repeat(__.out).times(3).count()", g);
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count() {
            ComputerTestHelper.compute("g.V.repeat(__.out).times(8).count()", g);
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_filterXfalseX_count() {
            ComputerTestHelper.compute("g.V.filter{false}.count", g)
        }
    }
}
