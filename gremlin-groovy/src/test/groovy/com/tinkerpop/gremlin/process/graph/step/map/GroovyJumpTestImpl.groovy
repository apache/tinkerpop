package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyJumpTestImpl extends JumpTest {

    @Override
    public Traversal<Vertex, String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(final Object v1Id) {
        g.v(v1Id).as('x').out.jump('x') { it.loops < 2 }.value('name')
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2X() {
        g.V.as('x').out.jump('x') { it.loops < 2 }
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX() {
        g.V.as('x').out.jump('x') { it.loops < 2 } { true }
    }

    @Override
    public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX_path() {
        g.V.as('x').out.jump('x') { it.loops < 2 } { true }.path
    }

    @Override
    public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2_trueX_path() {
        g.V.as('x').out.jump('x', 2) { true }.path
    }

    @Override
    public Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_loops_lt_2X_asXyX_in_jumpXy_loops_lt_2X_name() {
        g.V().as("x").out().jump('x') { it.loops < 2 }.as("y").in().jump("y") { it.loops < 2 }.name;
    }

    @Override
    public Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_2X_asXyX_in_jumpXy_2X_name() {
        g.V().as('x').out.jump('x', 2).as('y').in().jump('y', 2).value('name')
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2X() {
        return g.V.as('x').out().jump('x', 2);
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2_trueX() {
        return g.V.as('x').out.jump('x', 2) { true };
    }

    @Override
    public Traversal<Vertex, Path> get_g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path(
            final Object v1Id) {
        return g.v(v1Id).out.jump('x') { it.get().out.hasNext() }.in.jump("y").as("x").out.as('y').path;
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_jumpXxX_out_out_asXxX() {
        return g.V().jump("x").out.out.as("x");
    }

    @Override
    public Traversal<Vertex, String> get_g_v1_asXaX_jumpXb_loops_gt_1X_out_jumpXaX_asXbX_name(final Object v1Id) {
        g.v(v1Id).as('a').jump('b'){it.loops > 1}.out.jump('a').as('b').name
    }

}
