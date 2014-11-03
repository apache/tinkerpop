package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class UnfoldTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Edge> get_g_V_mapXoutEX_unfold();

    public abstract Traversal<Vertex, String> get_V_valueMap_unfold_mapXkeyX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_mapXoutEX_unfold() {
        final Traversal<Vertex, Edge> traversal = get_g_V_mapXoutEX_unfold();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            edges.add(traversal.next());
        }
        assertEquals(6, counter);
        assertEquals(6, edges.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMap_unfold_mapXkeyX() {
        final Traversal<Vertex, String> traversal = get_V_valueMap_unfold_mapXkeyX();
        printTraversalForm(traversal);
        int counter = 0;
        int ageCounter = 0;
        int nameCounter = 0;
        int langCounter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String key = traversal.next();
            if (key.equals("name"))
                nameCounter++;
            else if (key.equals("age"))
                ageCounter++;
            else if (key.equals("lang"))
                langCounter++;
            else
                fail("The provided key is not known: " + key);
        }
        assertEquals(12, counter);
        assertEquals(4, ageCounter);
        assertEquals(2, langCounter);
        assertEquals(6, nameCounter);
        assertEquals(counter, ageCounter + langCounter + nameCounter);
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends UnfoldTest {

        @Override
        public Traversal<Vertex, Edge> get_g_V_mapXoutEX_unfold() {
            return g.V().map(v -> v.get().outE()).unfold();
        }

        @Override
        public Traversal<Vertex, String> get_V_valueMap_unfold_mapXkeyX() {
            return g.V().valueMap().<Map.Entry<String, List>>unfold().map(m -> m.get().getKey());
        }
    }

    public static class ComputerTest extends UnfoldTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_mapXoutEX_unfold() {
            return (Traversal) g.V().map(v -> v.get().outE()).unfold().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_V_valueMap_unfold_mapXkeyX() {
            return g.V().valueMap().<Map.Entry<String, List>>unfold().map(m -> m.get().getKey()).submit(g.compute());
        }
    }
}
