package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovySelectTestImpl extends SelectTest {

    @Override
    public Traversal<Vertex, Map<String, Vertex>> get_g_v1_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
        g.v(v1Id).as('a').out('knows').as('b').select()
    }

    @Override
    public Traversal<Vertex, Map<String, String>> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(final Object v1Id) {
        g.v(v1Id).as('a').out('knows').as('b').select { it.value('name') }
    }

    @Override
    public Traversal<Vertex, Map<String, Vertex>> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
        g.v(v1Id).as('a').out('knows').as('b').select(['a'])
    }

    @Override
    public Traversal<Vertex, Map<String, String>> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(final Object v1Id) {
        g.v(v1Id).as('a').out('knows').as('b').select(['a']) { it.value('name') }
    }
}
