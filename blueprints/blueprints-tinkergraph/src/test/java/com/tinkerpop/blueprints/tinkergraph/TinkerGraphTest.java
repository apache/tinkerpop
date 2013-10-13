package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.computer.util.LambdaVertexProgram;
import org.junit.Test;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTest {

    @Test
    public void testTinkerGraph() {
        TinkerGraph g = new TinkerGraph();
        g.createIndex("name", Vertex.class);
        Vertex marko = g.addVertex(Property.of("name", "marko", "age", 33, "blah", "bloop"));
        Vertex stephen = g.addVertex(Property.of("name", "stephen", "id", 12, "blah", "bloop"));
        Random r = new Random();
        Stream.generate(() -> g.addVertex(Property.of(r.nextBoolean() + "1", r.nextInt(), "name", r.nextInt()))).limit(100000).count();
        assertEquals(g.vertices.size(), 100002);
        marko.addEdge("knows", stephen);
        System.out.println(g.query().has("name", Compare.EQUAL, "marko").vertices());
        System.out.println(marko.query().direction(Direction.OUT).labels("knows", "workedWith").vertices());
        g.createIndex("blah", Vertex.class);
    }

    @Test
    public void testLambdaProgram() {
        //TinkerGraph g = TinkerFactory.createClassic();
        TinkerGraph g = new TinkerGraph();
        Stream.generate(g::addVertex).limit(500000).count();
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
        /*StreamFactory.stream(g.query().vertices())
                .forEach(v -> System.out.println(result.getVertexMemory().getProperty(v, "i").getValue()));*/
    }

}
