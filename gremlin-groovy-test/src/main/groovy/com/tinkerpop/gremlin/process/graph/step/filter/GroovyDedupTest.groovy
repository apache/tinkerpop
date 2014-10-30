package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.groovy.loaders.GremlinLoader
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public abstract class GroovyDedupTest {

    public static class StandardTest extends DedupTest {
        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            g.V.both.dedup.name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_dedupXlangX_name() {
            g.V.both.dedup { it.get().property('lang').orElse(null) }.name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value() {
            g.V().both().properties('name').order { a, b -> a.get().value() <=> b.get().value() }.dedup.value
        }
    }

    public static class ComputerTestImpl extends DedupTest {
        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            ComputerTestHelper.compute("g.V.both.dedup.name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_dedupXlangX_name() {
            ComputerTestHelper.compute("g.V.both.dedup{it.get().property('lang').orElse(null)}.name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_propertiesXnameX_orderXa_bX_dedup_value() {
            ComputerTestHelper.compute("g.V.both.properties('name').order { a, b -> a.get().value() <=> b.get().value() }.dedup.value", g);
        }
    }
}
