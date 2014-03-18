package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AggregateTest extends AbstractGremlinTest {

    public abstract Iterator<Vertex> get_g_v1_aggregateXaX_outXcreatedX_inXcreatedX_exceptXaX(final Object v1Id);

    public abstract List<String> get_g_V_valueXnameX_aggregateXaX_iterate_getXaX();

    public abstract List<String> get_g_V_aggregateXa_nameX_iterate_getXaX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_aggregateXaX_outXcreatedX_inXcreatedX_exceptXaX() {
        final Iterator<Vertex> step = get_g_v1_aggregateXaX_outXcreatedX_inXcreatedX_exceptXaX(convertToId("marko"));
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            Vertex vertex = step.next();
            assertTrue(vertex.getValue("name").equals("peter") || vertex.getValue("name").equals("josh"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_valueXnameX_aggregateXaX_iterate_getXaX() {
        final List<String> names = get_g_V_valueXnameX_aggregateXaX_iterate_getXaX();
        assert_g_V_valueXnameX_aggregateXaX_iterate_getXaX(names);
    }

    private void assert_g_V_valueXnameX_aggregateXaX_iterate_getXaX(List<String> names) {
        assertEquals(6, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("ripple"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_aggregateXa_nameX_iterate_getXaX() {
        final List<String> names = get_g_V_aggregateXa_nameX_iterate_getXaX();
        assert_g_V_valueXnameX_aggregateXaX_iterate_getXaX(names);
    }

    public static class JavaAggregateTest extends AggregateTest {

        public Iterator<Vertex> get_g_v1_aggregateXaX_outXcreatedX_inXcreatedX_exceptXaX(final Object v1Id) {
            return g.v(v1Id).with("x", new HashSet<>()).aggregate("x").out("created").in("created").except("x");
        }

        public List<String> get_g_V_valueXnameX_aggregateXaX_iterate_getXaX() {
            return g.V().value("name").aggregate("x").iterate().memory().get("x");
        }

        public List<String> get_g_V_aggregateXa_nameX_iterate_getXaX() {
            return g.V().aggregate("a", v -> v.getValue("name")).iterate().memory().get("a");
        }
    }
}
