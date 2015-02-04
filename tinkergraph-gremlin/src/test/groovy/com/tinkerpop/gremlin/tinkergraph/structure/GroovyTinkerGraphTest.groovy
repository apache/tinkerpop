package com.tinkerpop.gremlin.tinkergraph.structure

import com.tinkerpop.gremlin.groovy.loaders.SugarLoader
import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.graph.traversal.GraphTraversal
import com.tinkerpop.gremlin.structure.Compare
import com.tinkerpop.gremlin.structure.Graph
import org.junit.Ignore
import org.junit.Test

import static com.tinkerpop.gremlin.process.graph.traversal.__.has
import static com.tinkerpop.gremlin.process.graph.traversal.__.out

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
        GraphTraversal t = g.V().or(
                out("knows"),
                has(T.label, "software").or(has("age", Compare.gte, 35))).values("name");

        System.out.println(t.toString());
        t.forEachRemaining { System.out.println(it) };
        System.out.println(t.toString());
        System.out.println(t.hasNext());
        // System.out.println(t.next().doubleValue() > 10.0d);

    }
}
