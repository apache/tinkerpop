package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ShuffleTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_shuffle_select();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXcreatedX_asXbX_shuffle_select() {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_asXaX_outXcreatedX_asXbX_shuffle_select();
        printTraversalForm(traversal);
        int counter = 0;
        int markoCounter = 0;
        int joshCounter = 0;
        int peterCounter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, Vertex> bindings = traversal.next();
            assertEquals(2, bindings.size());
            if (bindings.get("a").id().equals(convertToVertexId("marko"))) {
                assertEquals(convertToVertexId("lop"), bindings.get("b").id());
                markoCounter++;
            } else if (bindings.get("a").id().equals(convertToVertexId("josh"))) {
                assertTrue((bindings.get("b")).id().equals(convertToVertexId("lop")) || bindings.get("b").id().equals(convertToVertexId("ripple")));
                joshCounter++;
            } else if (bindings.get("a").id().equals(convertToVertexId("peter"))) {
                assertEquals(convertToVertexId("lop"), bindings.get("b").id());
                peterCounter++;
            } else {
                fail("This state should not have been reachable");
            }


        }
        assertEquals(4, markoCounter + joshCounter + peterCounter);
        assertEquals(1, markoCounter);
        assertEquals(1, peterCounter);
        assertEquals(2, joshCounter);
        assertEquals(4, counter);
    }

    public static class StandardTest extends ShuffleTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_shuffle_select() {
            return g.V().as("a").out("created").as("b").shuffle().select();
        }
    }

    public static class ComputerTest extends ShuffleTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_shuffle_select() {
            return (Traversal) g.V().as("a").out("created").as("b").shuffle().select().submit(g.compute());
        }
    }
}