package com.tinkerpop.gremlin.process.graph.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyDedupTestImpl extends DedupTest {

    public Traversal<Vertex, String> get_g_V_both_dedup_name() {
        g.V.both.dedup.value("name")
    }

    public Traversal<Vertex, String> get_g_V_both_dedupXlangX_name() {
        g.V.both.dedup{it.property("lang").orElse(null)}.value("name")
    }
}
