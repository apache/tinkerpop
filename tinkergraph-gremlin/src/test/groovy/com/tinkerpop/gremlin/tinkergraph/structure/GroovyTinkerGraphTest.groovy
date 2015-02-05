package com.tinkerpop.gremlin.tinkergraph.structure

import com.tinkerpop.gremlin.groovy.loaders.SugarLoader
import com.tinkerpop.gremlin.process.graph.traversal.GraphTraversal
import com.tinkerpop.gremlin.process.graph.traversal.__
import com.tinkerpop.gremlin.structure.Graph
import org.junit.Ignore
import org.junit.Test

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyTinkerGraphTest {

    static {
        SugarLoader.load();
    }

    @Test
    @Ignore
    public void testPlay3() throws Exception {
        Graph g = TinkerFactory.createModern();
        GraphTraversal t = SELECT 'a', 'b' FROM g.V.as('a').has(__.name & __.age.gt(29) | __.inE.count.gt(1l) & __.lang).'name'.as('b');

        System.out.println(t.toString());
        t.forEachRemaining { System.out.println(it) };
        System.out.println(t.toString());
    }
}
