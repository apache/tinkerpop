package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyJumpTestImpl extends JumpTest {

    public Traversal<Vertex, String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(final Object v1Id) {
        g.v(v1Id).as('x').out.jump('x') { it.loops < 2 }.value('name')
    }

}
