package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class FoldTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Iterator<Vertex>> get_g_V_fold();

    public abstract Traversal<Vertex, Vertex> get_g_V_fold_unfold();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_fold() {
        final Traversal<Vertex, Iterator<Vertex>> traversal = get_g_V_fold();
        System.out.println("Testing: " + traversal);
        Iterator<Vertex> itty = traversal.next();
        assertFalse(traversal.hasNext());
        Set<Vertex> vertices = new HashSet<>();
        itty.forEachRemaining(vertices::add);
        assertEquals(6, vertices.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_fold_unfold() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_fold_unfold();
        System.out.println("Testing: " + traversal);
        int count = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            vertices.add(traversal.next());
            count++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(6, count);
        assertEquals(6, vertices.size());
    }

    public static class JavaFoldTest extends FoldTest {

        public Traversal<Vertex, Iterator<Vertex>> get_g_V_fold() {
            return g.V().fold();
        }

        public Traversal<Vertex, Vertex> get_g_V_fold_unfold() {
            return g.V().fold().unfold();
        }
    }
}
