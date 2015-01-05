package com.tinkerpop.gremlin.process.graph.step.branch

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyRepeatTest {

    public static class StandardTest extends RepeatTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXloops_gte_2X_name(final Object v1Id) {
            g.V(v1Id).repeat(g.of().out).until { it.loops() >= 2 }.name
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X() {
            g.V.repeat(g.of().out).until { it.loops() >= 2 }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X_emit() {
            g.V.repeat(g.of().out).until { it.loops() >= 2 }.emit
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path() {
            g.V.repeat(g.of().out).until { it.loops() >= 2 }.emit.path
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilX2X_emit_path() {
            g.V.repeat(g.of().out).until(2).emit.path
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name() {
            g.V.repeat(g.of().out).until { it.loops() >= 2 }.repeat(g.of().in).until { it.loops() >= 2 }.name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilX2X_repeatXinX_untilX2X_name() {
            g.V.repeat(g.of().out).until(2).repeat(g.of().in).until(2).name
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilX2X() {
            g.V.repeat(g.of().out).until(2)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilX2X_emit() {
            g.V.repeat(g.of().out).until(2).emit;
        }
    }

    public static class ComputerTest extends RepeatTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXloops_gte_2X_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).repeat(g.of().out).until { it.loops() >= 2 }.name", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X() {
            ComputerTestHelper.compute("g.V.repeat(g.of().out).until { it.loops() >= 2 }", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X_emit() {
            ComputerTestHelper.compute("g.V.repeat(g.of().out).until { it.loops() >= 2 }.emit", g)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path() {
            ComputerTestHelper.compute("g.V.repeat(g.of().out).until { it.loops() >= 2 }.emit.path", g)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilX2X_emit_path() {
            ComputerTestHelper.compute("g.V.repeat(g.of().out).until(2).emit.path", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name() {
            ComputerTestHelper.compute("g.V.repeat(g.of().out).until { it.loops() >= 2 }.repeat(g.of().in).until { it.loops() >= 2 }.name", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilX2X_repeatXinX_untilX2X_name() {
            ComputerTestHelper.compute("g.V.repeat(g.of().out).until(2).repeat(g.of().in).until(2).name", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilX2X() {
            ComputerTestHelper.compute("g.V.repeat(g.of().out).until(2)", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilX2X_emit() {
            ComputerTestHelper.compute("g.V.repeat(g.of().out).until(2).emit", g)
        }
    }
}
