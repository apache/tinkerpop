package com.tinkerpop.gremlin.olap;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.StreamFactory;
import com.tinkerpop.gremlin.tinkergraph.TinkerFactory;
import org.junit.Test;

import java.util.Iterator;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgramTest {

    /*@Test
    public void testGremlinOLAP() throws Exception {
        final Graph g = TinkerFactory.createClassic();
        final ComputeResult result =
                g.compute().program(TraversalVertexProgram.create().gremlin(() -> (GremlinJ)
                        //Gremlin.of().out("created").in("created").value("name").map(o -> o.toString().length()))
                        //Gremlin.of().out().out().property("name").value().path())
                        GremlinJ.of().V().as("x").outE().inV().jump("x", o -> o.getLoops() < 2).value("name").map(s -> s.toString().length()).path())
                        .build())
                        .submit().get();

        /////////// GREMLIN REPL LOOK

        if (result.getGraphMemory().get(TraversalVertexProgram.TRACK_PATHS)) {
            System.out.println("gremlin> " + result.getGraphMemory().<Supplier>get("gremlinPipeline").get());
            StreamFactory.stream(g.query().vertices()).forEach(v -> {
                final TraversalPaths tracker = result.getVertexMemory().<TraversalPaths>getProperty(v, TraversalVertexProgram.GREMLIN_TRACKER).get();
                tracker.getDoneGraphTracks().forEach((a, b) -> Stream.generate(() -> 1).limit(((List) b).size()).forEach(t -> System.out.println("==>" + a)));
                tracker.getDoneObjectTracks().forEach((a, b) -> Stream.generate(() -> 1).limit(((List) b).size()).forEach(t -> System.out.println("==>" + a)));
            });
        } else {
            System.out.println("gremlin> " + result.getGraphMemory().<Supplier>get("gremlinPipeline").get());
            StreamFactory.stream(g.query().vertices()).forEach(v -> {
                final TraversalCounters tracker = result.getVertexMemory().<TraversalCounters>getProperty(v, TraversalVertexProgram.GREMLIN_TRACKER).get();
                tracker.getDoneGraphTracks().forEach((a, b) -> Stream.generate(() -> 1).limit(b).forEach(t -> System.out.println("==>" + a)));
                tracker.getDoneObjectTracks().forEach((a, b) -> Stream.generate(() -> 1).limit(b).forEach(t -> System.out.println("==>" + a)));
            });
        }
    }*/

    @Test
    public void testIterable() throws Exception {
        final Graph g = TinkerFactory.createClassic();
        final BiFunction<String, Iterator<Integer>, Long> reduction = (k, v) -> StreamFactory.stream(v).count();
        //   new TraversalResult<>(g, () -> GremlinJ.of().v("1").both().both().value("name")).forEachRemaining(System.out::println);

    }
}
