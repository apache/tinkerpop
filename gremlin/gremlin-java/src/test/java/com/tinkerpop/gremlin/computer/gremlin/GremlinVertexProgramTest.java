package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.pipes.util.Holder;
import org.junit.Test;

import java.util.HashMap;
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
                        //Gremlin.of().out("created").in("created").value("name").map(h -> ((String) ((Holder) h).get()).length()).filter(h -> ((Integer) ((Holder) h).get() > 4)).identity())
                        Gremlin.of().identity().as("x").out().loop("x", o -> ((Holder)o).getLoops() < 2, o -> false))
                        .build())
                        .submit().get();

        /////////// GREMLIN REPL LOOK

        System.out.println("gremlin> " + result.getGraphMemory().<Supplier>get("gremlinPipeline").get());
        StreamFactory.stream(g.query().vertices()).forEach(v -> {
            result.getVertexMemory().getProperty(v, GremlinVertexProgram.GRAPH_GREMLINS).ifPresent(m -> ((HashMap<Object, List<Holder>>) m).forEach((a, b) -> Stream.generate(() -> 1).limit(((List) b).size()).forEach(t -> System.out.println("==>" + a))));
            result.getVertexMemory().getProperty(v, GremlinVertexProgram.OBJECT_GREMLINS).ifPresent(m -> ((HashMap<Object, List<Holder>>) m).forEach((a, b) -> Stream.generate(() -> 1).limit(((List) b).size()).forEach(t -> System.out.println("==>" + a))));
        });

        /////////// FULL GRAPH LOOK
        /*
        System.out.println("\n\n[--VERTICES--]");
        StreamFactory.stream(g.query().vertices()).forEach(v -> {
            System.out.println(v.getId() + " > " + result.getVertexMemory().getProperty(v, GremlinVertexProgram.GRAPH_GREMLINS).orElse("."));
            System.out.println("\t" + "[others] > " + result.getVertexMemory().getProperty(v, GremlinVertexProgram.OBJECT_GREMLINS).orElse("."));
            v.getProperties().values().forEach(p -> {
                System.out.println("\t" + p.getKey() + ":" + p.getValue() + " > " + result.getVertexMemory().getAnnotation(p, GremlinVertexProgram.GRAPH_GREMLINS).orElse("."));
            });
        });

        System.out.println("\n[--EDGES--]");
        StreamFactory.stream(g.query().edges()).forEach(e -> {
            System.out.println(e.getId() + " > " + result.getVertexMemory().getProperty(e, GremlinVertexProgram.GRAPH_GREMLINS).orElse("."));
            e.getProperties().values().forEach(p -> {
                System.out.println("\t" + p.getKey() + ":" + p.getValue() + " > " + result.getVertexMemory().getAnnotation(p, GremlinVertexProgram.GRAPH_GREMLINS).orElse("."));
            });
        });*/

    }
}
