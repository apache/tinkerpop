package com.tinkerpop.gremlin.computer;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgramTest extends TestCase {

    public void testGremlinOLAP() {
        Graph g = TinkerFactory.createClassic();
        ComputeResult result =
                g.compute().program(GremlinVertexProgram.create().gremlin(() -> (Gremlin)
                        Gremlin.of().has("name", "marko").out().out().simplePath().has("name","lop").in())
                        .build())
                        .submit();

        StreamFactory.stream(g.query().vertices()).forEach(v -> {
            System.out.println(v.getId() + ": " + result.getVertexMemory().getProperty(v, "gremlins").orElseGet(() -> null));
        });


    }
}
