package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SimplePathTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_simplePath(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXcreatedX_inXcreatedX_simplePath() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outXcreatedX_inXcreatedX_simplePath(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            assertTrue(vertex.getValue("name").equals("josh") || vertex.getValue("name").equals("peter"));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    public static class JavaSimplePathTest extends SimplePathTest {

        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_simplePath(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").simplePath();
        }
    }

    public static class JavaComputerSimplePathTest extends SimplePathTest {

        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_simplePath(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").simplePath().submit(g.compute());
        }
    }
}
