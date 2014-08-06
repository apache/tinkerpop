package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyPathTestImpl extends PathTest {

    public Traversal<Vertex, Path> get_g_v1_valueXnameX_path(final Object v1Id) {
        g.v(v1Id).identity().value('name').path
    }

    public Traversal<Vertex, Path> get_g_v1_out_pathXage_nameX(final Object v1Id) {
        g.v(v1Id).out.path { it.value('age') } { it.value('name') }
    }

    public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_pathXit__name__langX() {
        g.V.as('x').out.jump('x') { it.loops < 2 }.path { it } { it.value('name') } { it.value('lang') }
    }

    public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_pathXit_name_langX() {
        g.V.as('x').out.jump('x', 2).path { it } { it.value('name') } { it.value('lang') }
    }

    public Traversal<Vertex, Path> get_g_V_out_out_pathXname_ageX() {
        g.V().out.out().path { it.value('name') } { it.value('age') }
    }
}
