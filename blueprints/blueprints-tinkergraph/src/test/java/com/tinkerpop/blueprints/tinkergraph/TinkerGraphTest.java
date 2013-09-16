package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTest extends TestCase {

    public void testTinkerGraph() {
        TinkerGraph g = new TinkerGraph();
        g.createIndex("name", Vertex.class);
        Vertex marko = g.addVertex(TinkerProperty.make("name", "marko", "age", 33));
        Vertex stephen = g.addVertex(TinkerProperty.make("name", "stephen", "id", 12));
        marko.addEdge("knows", stephen);
        System.out.println(g.query().has("name", Compare.EQUAL, "marko").vertices());
        System.out.println(marko.query().direction(Direction.OUT).labels("knows", "workedWith").vertices());
    }
}
