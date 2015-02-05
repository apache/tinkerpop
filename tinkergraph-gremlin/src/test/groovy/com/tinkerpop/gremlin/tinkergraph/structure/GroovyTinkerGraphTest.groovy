package com.tinkerpop.gremlin.tinkergraph.structure

import com.tinkerpop.gremlin.groovy.loaders.SugarLoader
import com.tinkerpop.gremlin.process.graph.traversal.GraphTraversal
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
        GraphTraversal t = select('a', 'b') from g.V.as('a').has(name & age.gt(29) | inE.count.gt(1l) & lang).name.as('b');

        System.out.println(t.toString());
        t.forEachRemaining { System.out.println(it) };
        System.out.println(t.toString());
    }
}
