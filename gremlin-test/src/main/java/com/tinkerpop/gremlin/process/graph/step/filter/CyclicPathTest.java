package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class CyclicPathTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_cyclicPath(final Object v1);

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXcreatedX_inXcreatedX_simplePath() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outXcreatedX_inXcreatedX_cyclicPath(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            assertEquals("marko", vertex.<String>value("name"));
        }
        assertEquals(1, counter);
        assertFalse(traversal.hasNext());
    }

    public static class JavaCyclicPathTest extends CyclicPathTest {

        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_cyclicPath(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").cyclicPath();
        }
    }

    public static class JavaComputerCyclicPathTest extends CyclicPathTest {
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_cyclicPath(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").cyclicPath().submit(g.compute());
        }
    }
}
