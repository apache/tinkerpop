package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import org.junit.Test;

import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgramTest {

    public void testTrue() {

    }

    @Test
    public void testGremlinOLAP() throws Exception {
        Graph g = TinkerFactory.createClassic();
        ComputeResult result =
                g.compute().program(GremlinVertexProgram.create().gremlin(() -> (Gremlin)
                        //Gremlin.of().out("created").in("created").property("name"))
                        Gremlin.of().as("x").outE().inV().loop("x", o -> o.getLoops() < 2).value("name").map(s -> s.toString().length()).path())
                        .build())
                        .submit().get();

        /////////// GREMLIN REPL LOOK

        System.out.println("gremlin> " + result.getGraphMemory().<Supplier>get("gremlinPipeline").get());
        StreamFactory.stream(g.query().vertices()).forEach(v -> {
            final GremlinTracker tracker = result.getVertexMemory().<GremlinTracker>getProperty(v, GremlinVertexProgram.GREMLIN_TRACKER).get();
            tracker.getDoneGraphHolders().forEach((a, b) -> Stream.generate(() -> 1).limit(b.size()).forEach(t -> System.out.println("==>" + a)));
            tracker.getDoneObjectHolders().forEach((a, b) -> Stream.generate(() -> 1).limit(b.size()).forEach(t -> System.out.println("==>" + a)));
        });
    }
}
