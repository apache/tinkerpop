package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import org.junit.Test;

import java.util.List;
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
                        //Gremlin.of().out("created").in("created").value("name").map(o -> o.toString().length()))
                        Gremlin.of().out().out().property("name").value().path())
                        //Gremlin.of().as("x").outE().inV().loop("x", o -> o.getLoops() < 2).value("name").map(s -> s.toString().length()).path())
                        .build())
                        .submit().get();

        /////////// GREMLIN REPL LOOK

        if (result.getGraphMemory().get(GremlinVertexProgram.TRACK_PATHS)) {
            System.out.println("gremlin> " + result.getGraphMemory().<Supplier>get("gremlinPipeline").get());
            StreamFactory.stream(g.query().vertices()).forEach(v -> {
                final GremlinPaths tracker = result.getVertexMemory().<GremlinPaths>getProperty(v, GremlinVertexProgram.GREMLIN_TRACKER).get();
                tracker.getDoneGraphTracks().forEach((a, b) -> Stream.generate(() -> 1).limit(((List) b).size()).forEach(t -> System.out.println("==>" + a)));
                tracker.getDoneObjectTracks().forEach((a, b) -> Stream.generate(() -> 1).limit(((List) b).size()).forEach(t -> System.out.println("==>" + a)));
            });
        } else {
            System.out.println("gremlin> " + result.getGraphMemory().<Supplier>get("gremlinPipeline").get());
            StreamFactory.stream(g.query().vertices()).forEach(v -> {
                final GremlinCounter tracker = result.getVertexMemory().<GremlinCounter>getProperty(v, GremlinVertexProgram.GREMLIN_TRACKER).get();
                tracker.getDoneGraphTracks().forEach((a, b) -> Stream.generate(() -> 1).limit(b).forEach(t -> System.out.println("==>" + a)));
                tracker.getDoneObjectTracks().forEach((a, b) -> Stream.generate(() -> 1).limit(b).forEach(t -> System.out.println("==>" + a)));
            });
        }
    }
}
