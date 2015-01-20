package com.tinkerpop.gremlin.process.graph.step.branch

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

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
            g.V(v1Id).repeat(__.out).until { it.loops() >= 2 }.name
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X() {
            g.V.repeat(__.out).until { it.loops() >= 2 }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X_emit() {
            g.V.repeat(__.out).until { it.loops() >= 2 }.emit
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path() {
            g.V.repeat(__.out).until { it.loops() >= 2 }.emit.path
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_emit_path() {
            g.V.repeat(__.out).times(2).emit.path
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name() {
            g.V.repeat(__.out).until { it.loops() >= 2 }.repeat(__.in).until { it.loops() >= 2 }.name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name() {
            g.V.repeat(__.out).times(2).repeat(__.in).times(2).name
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X() {
            g.V.repeat(__.out).times(2)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X_emit() {
            g.V.repeat(__.out).times(2).emit;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_untilXloops_gte_2X_repeatXoutX_name(Object v1Id) {
            g.V(v1Id).until { it.loops() >= 2 }.repeat(__.out).name
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_timesX2X_repeatXoutX_name(Object v1Id) {
            g.V(v1Id).times(2).repeat(__.out).name
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_repeatXoutX_timesX2X_path() {
            g.V.emit.repeat(__.out).times(2).path
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_timesX2X_repeatXoutX_path() {
            g.V.emit.times(2).repeat(__.out).path
        }

        @Override
        public Traversal<Vertex, String> get_g_V_emitXhasXlabel_personXX_repeatXoutX_name(final Object v1Id) {
            g.V(v1Id).emit(__.has(T.label, 'person')).repeat(__.out).name
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX() {
            g.V.repeat(__.groupCount('m').by('name').out).times(2).cap('m')
        }
    }

    public static class ComputerTest extends RepeatTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXloops_gte_2X_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).repeat(__.out).until { it.loops() >= 2 }.name", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X() {
            ComputerTestHelper.compute("g.V.repeat(__.out).until { it.loops() >= 2 }", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X_emit() {
            ComputerTestHelper.compute("g.V.repeat(__.out).until { it.loops() >= 2 }.emit", g)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path() {
            ComputerTestHelper.compute("g.V.repeat(__.out).until { it.loops() >= 2 }.emit.path", g)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_emit_path() {
            ComputerTestHelper.compute("g.V.repeat(__.out).times(2).emit.path", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name() {
            ComputerTestHelper.compute("g.V.repeat(__.out).until { it.loops() >= 2 }.repeat(__.in).until { it.loops() >= 2 }.name", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name() {
            ComputerTestHelper.compute("g.V.repeat(__.out).times(2).repeat(__.in).times(2).name", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X() {
            ComputerTestHelper.compute("g.V.repeat(__.out).times(2)", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X_emit() {
            ComputerTestHelper.compute("g.V.repeat(__.out).times(2).emit", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_untilXloops_gte_2X_repeatXoutX_name(Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).until{it.loops() >= 2}.repeat(__.out).name", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_timesX2X_repeatXoutX_name(Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).times(2).repeat(__.out).name", g)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_repeatXoutX_timesX2X_path() {
            ComputerTestHelper.compute("g.V.emit.repeat(__.out).times(2).path", g)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_timesX2X_repeatXoutX_path() {
            ComputerTestHelper.compute("g.V.emit.times(2).repeat(__.out).path", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_emitXhasXlabel_personXX_repeatXoutX_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).emit(__.has(T.label, 'person')).repeat(__.out).name", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX() {
            ComputerTestHelper.compute("g.V.repeat(__.groupCount('m').by('name').out).times(2).cap('m')", g)
        }
    }
}
