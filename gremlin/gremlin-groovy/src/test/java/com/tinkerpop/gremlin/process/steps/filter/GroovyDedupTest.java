package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyDedupTest extends DedupTest {

    public Traversal<Vertex, String> get_g_V_both_dedup_name() {
        return g.V().both().dedup().value("name");
    }

    public Traversal<Vertex, String> get_g_V_both_dedupXlangX_name() {
        return g.V().both().dedup(v -> v.getProperty("lang").orElse(null)).value("name");
    }
}
