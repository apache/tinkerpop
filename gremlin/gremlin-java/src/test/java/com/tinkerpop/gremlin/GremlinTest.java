package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.pipes.Gremlin;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinTest extends TestCase {

    public void testPipeline() {

        TinkerGraph g = TinkerFactory.createClassic();
        new Gremlin(g.query().vertices()).identity().as("x")
                .both("knows")
                .has("age")
                .sideEffect(System.out::println)
                .loop("x", o -> true, o -> true);


        //new Gremlin<Integer, Integer>(Arrays.asList(1, 1, 1, 2, 3)).dedup().sideEffect(System.out::println).iterate();

    }
}
