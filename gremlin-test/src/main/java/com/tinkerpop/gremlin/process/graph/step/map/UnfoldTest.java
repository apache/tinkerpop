package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class UnfoldTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Edge> get_g_V_mapXoutEX_unfold();

    @Test
    @LoadGraphWith(CLASSIC)
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

    public static class JavaUnfoldTest extends UnfoldTest {

        public Traversal<Vertex, Edge> get_g_V_mapXoutEX_unfold() {
            return g.V().map(t -> t.get().outE()).unfold();
        }
    }

    public static class JavaComputerUnfoldTest extends UnfoldTest {

        public Traversal<Vertex, Edge> get_g_V_mapXoutEX_unfold() {
            return (Traversal) g.V().map(t -> t.get().outE()).unfold().submit(g.compute());
        }
    }
}
