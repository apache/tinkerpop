package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class ValueMapTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, List>> get_g_V_valueMap();

    public abstract Traversal<Vertex, Map<String, List>> get_g_V_valueMapXname_ageX();

    public abstract Traversal<Vertex, Map<String, List<String>>> get_g_VX1X_outXcreatedX_valueMap(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMap() {
        final Traversal<Vertex, Map<String, List>> traversal = get_g_V_valueMap();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, List> values = traversal.next();
            final String name = (String) values.get("name").get(0);
            assertEquals(2, values.size());
            if (name.equals("marko")) {
                assertEquals(29, values.get("age").get(0));
            } else if (name.equals("josh")) {
                assertEquals(32, values.get("age").get(0));
            } else if (name.equals("peter")) {
                assertEquals(35, values.get("age").get(0));
            } else if (name.equals("vadas")) {
                assertEquals(27, values.get("age").get(0));
            } else if (name.equals("lop")) {
                assertEquals("java", values.get("lang").get(0));
            } else if (name.equals("ripple")) {
                assertEquals("java", values.get("lang").get(0));
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMapXname_ageX() {
        final Traversal<Vertex, Map<String, List>> traversal = get_g_V_valueMapXname_ageX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, List> values = traversal.next();
            final String name = (String) values.get("name").get(0);
            if (name.equals("marko")) {
                assertEquals(29, values.get("age").get(0));
                assertEquals(2, values.size());
            } else if (name.equals("josh")) {
                assertEquals(32, values.get("age").get(0));
                assertEquals(2, values.size());
            } else if (name.equals("peter")) {
                assertEquals(35, values.get("age").get(0));
                assertEquals(2, values.size());
            } else if (name.equals("vadas")) {
                assertEquals(27, values.get("age").get(0));
                assertEquals(2, values.size());
            } else if (name.equals("lop")) {
                assertNull(values.get("lang"));
                assertEquals(1, values.size());
            } else if (name.equals("ripple")) {
                assertNull(values.get("lang"));
                assertEquals(1, values.size());
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXcreatedX_valueMap() {
        final Traversal<Vertex, Map<String, List<String>>> traversal = get_g_VX1X_outXcreatedX_valueMap(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, List<String>> values = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals("lop", values.get("name").get(0));
        assertEquals("java", values.get("lang").get(0));
        assertEquals(2, values.size());
    }

    public static class StandardTest extends ValueMapTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_valueMap() {
            return g.V().valueMap();
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_valueMapXname_ageX() {
            return g.V().valueMap("name", "age");
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_VX1X_outXcreatedX_valueMap(final Object v1Id) {
            return g.V(v1Id).out("created").valueMap();
        }
    }

    public static class ComputerTest extends ValueMapTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_valueMap() {
            return (Traversal) g.V().valueMap().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_valueMapXname_ageX() {
            return (Traversal) g.V().valueMap("name", "age").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_VX1X_outXcreatedX_valueMap(final Object v1Id) {
            return (Traversal) g.V(v1Id).out("created").valueMap().submit(g.compute());
        }
    }
}
