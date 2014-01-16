package com.tinkerpop.gremlin.pipes.filter;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.test.ComplianceTest;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DedupTest extends com.tinkerpop.gremlin.test.filter.DedupTest {

    final Graph g = TinkerFactory.createClassic();

    @Test
    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }

    @Test
    public void g_V_both_dedup_name() {
        super.g_V_both_dedup_name(Gremlin.of(g).V().both().dedup().value("name"));
    }

    @Test
    public void g_V_both_dedupXlangX_name() {
        super.g_V_both_dedupXlangX_name(Gremlin.of(g).V().both().dedup(v -> v.get().getProperty("lang").orElse(null)).value("name"));
    }
}
