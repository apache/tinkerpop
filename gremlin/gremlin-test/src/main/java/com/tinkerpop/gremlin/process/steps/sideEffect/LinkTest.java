package com.tinkerpop.gremlin.process.steps.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class LinkTest extends AbstractGremlinTest {
    public abstract Iterator<Vertex> get_g_v1_asXaX_outXcreatedX_inXcreatedX_linkBothXcocreator_aX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXaX_outXcreatedX_inXcreatedX_linkBothXcocreator_aX() {
        final Iterator<Vertex> step = get_g_v1_asXaX_outXcreatedX_inXcreatedX_linkBothXcocreator_aX();
        System.out.println("Testing: " + step);
        final List<Vertex> cocreators = new ArrayList<>();
        final List<Object> ids = new ArrayList<>();
        while (step.hasNext()) {
            final Vertex vertex = step.next();
            cocreators.add(vertex);
            ids.add(vertex.getId());
        }
        assertEquals(cocreators.size(), 3);
        assertTrue(ids.contains("1"));
        assertTrue(ids.contains("6"));
        assertTrue(ids.contains("4"));

        for (Vertex vertex : cocreators) {
            if (vertex.getId().equals("1")) {
                assertEquals(vertex.outE("cocreator").count(), 4);
                assertEquals(vertex.inE("cocreator").count(), 4);
            } else {
                assertEquals(vertex.outE("cocreator").count(), 1);
                assertEquals(vertex.inE("cocreator").count(), 1);
            }
        }
    }

    public static class JavaLinkTest extends LinkTest {

        public Iterator<Vertex> get_g_v1_asXaX_outXcreatedX_inXcreatedX_linkBothXcocreator_aX() {
            return g.v(1).as("a").out("created").in("created").linkBoth("cocreator", "a");
        }
    }
}
