package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
class GroovyDedupTestImpl extends DedupTest {

    @Override
    public Traversal<Vertex, String> get_g_V_both_dedup_name() {
        g.V().both().dedup().value("name")
    }

    @Override
    public Traversal<Vertex, String> get_g_V_both_dedupXlangX_name() {
        g.V().both().dedup { it.property("lang").orElse(null) }.value("name")
    }

    @Override
    public Traversal<Vertex, String> get_g_V_both_name_orderXa_bX_dedup() {
        g.V().both().value("name").order { a, b -> a.get() <=> b.get() }.dedup()
    }
}
