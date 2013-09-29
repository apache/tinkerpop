package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinTest extends TestCase {

    public void testPipeline() {

        TinkerGraph g = TinkerFactory.createClassic();
        new Gremlin<Vertex, List>(g.query().vertices().iterator())
                .out("knows").out("created")
                .has("name")
                .value("name").path()
                .sideEffect(System.out::println).iterate();
    }
}
