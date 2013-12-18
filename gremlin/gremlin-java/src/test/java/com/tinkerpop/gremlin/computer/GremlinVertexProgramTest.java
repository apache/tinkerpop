package com.tinkerpop.gremlin.computer;

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
                        Gremlin.of().has("name", "marko").out("created").in("created").out("created"))
                        .build())
                        .submit().get();

        StreamFactory.stream(g.query().vertices()).forEach(v -> {
            System.out.println(v.getId() + ": " + result.getVertexMemory().getProperty(v, "gremlins").orElseGet(() -> null));
        });


    }
}
