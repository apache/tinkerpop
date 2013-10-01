package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.pipes.util.Holder;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinTest extends TestCase {

    public void testPipeline() {

        TinkerGraph g = TinkerFactory.createClassic();
        new Gremlin<Vertex, List>(g.query().vertices())
                .out("knows").out("created")
                .has("name")
                .value("name").path()
                .sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        new Gremlin<Vertex, Vertex>(g.query().vertices()).as("x").out("knows").back("x").sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        new Gremlin<>(g.query().vertices()).as("x").out()
                .loop("x", o -> ((Holder) o).getLoops() < 3, o -> false)
                .sideEffect(o -> System.out.println(((Holder) o).getLoops()))
                .path().sideEffect(System.out::println).iterate();

        System.out.println("--------------");

        System.out.println(new Gremlin<Vertex, List>(g.query().vertices())
                .both().groupCount());
    }
}
