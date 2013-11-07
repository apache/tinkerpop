package com.tinkerpop.gremlin.computer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgramTest {

    public void testTrue() {

    }
    /*@Test
    public void testGremlinOLAP() {
        Graph g = TinkerFactory.createClassic();
        ComputeResult result =
                g.compute().program(GremlinVertexProgram.create().gremlin(() -> (Gremlin)
                        Gremlin.of().has("name", "marko").out().out())
                        .build())
                        .submit();

        StreamFactory.stream(g.query().vertices()).forEach(v -> {
            System.out.println(v.getId() + ": " + result.getVertexMemory().getProperty(v, "gremlins").orElseGet(() -> null));
        });


    }*/
}
