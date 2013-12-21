package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import org.junit.Test;

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
                        Gremlin.of().has("name", "marko").outE("knows").inV().property("name").identity())
                        .build())
                        .submit().get();

        System.out.println("---VERTICES---");
        StreamFactory.stream(g.query().vertices()).forEach(v -> {
            System.out.println(v.getId() + "==>" + result.getAnnotationMemory().getElementAnnotation(v, "gremlins").orElse(null));
            v.getProperties().values().forEach(p -> {
                System.out.println("\t" + p.getKey() + ":" + p.getValue() + "==>" + p.getAnnotation("gremlins").orElse(null));
            });
        });

        System.out.println("---EDGES---");
        StreamFactory.stream(g.query().edges()).forEach(e -> {
            System.out.println(e.getId() + "==>" + result.getAnnotationMemory().getElementAnnotation(e, "gremlins").orElse(null));
        });


    }
}
