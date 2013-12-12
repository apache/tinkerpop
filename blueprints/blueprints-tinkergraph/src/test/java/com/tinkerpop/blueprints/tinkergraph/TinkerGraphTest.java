package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTest {

    @Test
    public void testTinkerGraph() {
        final TinkerGraph g = TinkerGraph.open(Optional.empty());
        g.createIndex("name", Vertex.class);
        final Vertex marko = g.addVertex("name", "marko", "age", 33, "blah", "bloop");
        final Vertex stephen = g.addVertex("name", "stephen", "id", 12, "blah", "bloop");
        stephen.setProperty(Property.Key.hidden("name"), "stephen");
        assertEquals("stephen", stephen.getProperty(Property.Key.hidden("name")).getValue());
        final Random r = new Random();
        Stream.generate(() -> g.addVertex(r.nextBoolean() + "1", r.nextInt(), "name", r.nextInt())).limit(100000).count();
        assertEquals(100002, g.vertices.size());
        marko.addEdge("knows", stephen);
        System.out.println(g.query().has("name", Compare.EQUAL, "marko").vertices());
        System.out.println(marko.query().direction(Direction.OUT).labels("knows", "workedWith").vertices());
        g.createIndex("blah", Vertex.class);
    }

    /*@Test
    public void testLambdaProgram() {
        TinkerGraph g = TinkerGraph.open(Optional.empty());
        Stream.generate(g::addVertex).limit(5000).count();
        ComputeResult result = g.compute().program(LambdaVertexProgram.create()
                .setup(gm -> {
                })
                .execute((v, gm) -> {
                    v.setProperty("i", gm.getIteration());
                })
                .terminate(gm -> gm.getIteration() > 20)
                .computeKeys(VertexProgram.ofComputeKeys("i", VertexProgram.KeyType.VARIABLE))
                .build())
                .submit();

        System.out.println("Runtime: " + result.getGraphMemory().getRuntime());
        StreamFactory.stream(g.query().vertices())
                .forEach(v -> System.out.println(result.getVertexMemory().getProperty(v, "i").getValue()));
    }*/

}
