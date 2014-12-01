package com.tinkerpop.gremlin.process.computer.bulk;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import org.junit.Test;

import java.util.Iterator;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SaveGraphVertexProgramTest extends AbstractGremlinProcessTest {

    public SaveGraphVertexProgramTest() {
        requiresGraphComputer = true;
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldExecutePageRank() throws Exception {
        final ComputerResult result = g.compute().program(SaveGraphVertexProgram.build().create()).submit().get();
        result.graph().V().forEachRemaining(v -> {
            assertTrue(v.keys().contains("name"));
            assertTrue(v.hiddenKeys().contains(SaveGraphVertexProgram.VERTEX_SERIALIZATION));
            System.out.println(v.id() + "-->" + v.value(SaveGraphVertexProgram.VERTEX_SERIALIZATION));
        });
        assertEquals(result.memory().getIteration(), 0);
        assertEquals(result.memory().asMap().size(), 1);
        System.out.println("-----------");
        Iterator<String> itty = result.memory().get(SaveGraphMapReduce.SIDE_EFFECT_KEY);
        while(itty.hasNext()) {
            System.out.println(itty.next());
        }
    }

}