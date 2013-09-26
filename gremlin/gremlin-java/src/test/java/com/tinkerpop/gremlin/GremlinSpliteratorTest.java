package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import junit.framework.TestCase;

import java.util.stream.StreamSupport;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinSpliteratorTest extends TestCase {

    public void testStuff() {
        TinkerGraph g = TinkerFactory.createClassic();
        StreamSupport.stream(new GremlinSpliterator<>(g.query().vertices()), true).map(v -> v.getValue("name")).forEach(System.out::println);
    }
}
