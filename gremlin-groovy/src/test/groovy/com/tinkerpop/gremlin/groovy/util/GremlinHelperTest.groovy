package com.tinkerpop.gremlin.groovy.util

import com.tinkerpop.gremlin.groovy.loaders.GremlinLoader
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import org.junit.Ignore
import org.junit.Test

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GremlinHelperTest {

    static {
        GremlinLoader.load();
    }

    @Test
    @Ignore
    public void shouldReturnValidClosureString() {
        Graph g = TinkerFactory.createClassic();
        println GremlinHelper.getClosureString { 1 + 2 }
    }
}
