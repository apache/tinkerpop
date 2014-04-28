package com.tinkerpop.gremlin.process.graph.filter

import com.tinkerpop.gremlin.groovy.GremlinLoader
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyDedupTest extends DedupTest {

    static {
        GremlinLoader.load();
    }

    public Traversal<Vertex, String> get_g_V_both_dedup_name() {
        return g.V.both.dedup.value("name");
    }

    public Traversal<Vertex, String> get_g_V_both_dedupXlangX_name() {
        return g.V.both.dedup{it.getProperty("lang").orElse(null)}.value("name");
    }
}
