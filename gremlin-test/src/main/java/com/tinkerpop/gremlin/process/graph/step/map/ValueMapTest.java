package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
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

    public abstract Traversal<Edge, Map<String, Object>> get_g_E_valueMapXid_label_weightX();

    public abstract Traversal<Vertex, Map<String, List<String>>> get_g_v1_outXcreatedX_valueMap(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_values() {
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
    public void g_V_valuesXname_ageX() {
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
    @Ignore
    @LoadGraphWith(MODERN)
    public void g_E_valuesXid_label_weightX() {
        final Traversal<Edge, Map<String, Object>> traversal = get_g_E_valueMapXid_label_weightX();
        printTraversalForm(traversal);
        int counter = 0;
        int counter2 = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Object> values = traversal.next();
            if (values.get("label").equals("knows") && values.get("weight").equals(0.5d) && values.size() == 3)
                counter2++;
            else if (values.get("label").equals("knows") && values.get("weight").equals(1.0d) && values.size() == 3)
                counter2++;
            else if (values.get("label").equals("created") && values.get("weight").equals(0.4d) && values.size() == 3)
                counter2++;
            else if (values.get("label").equals("created") && values.get("weight").equals(1.0d) && values.size() == 3)
                counter2++;
            else if (values.get("label").equals("created") && values.get("weight").equals(0.4d) && values.size() == 3)
                counter2++;
            else if (values.get("label").equals("created") && values.get("weight").equals(0.2d) && values.size() == 3)
                counter2++;
        }
        assertEquals(6, counter2);
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outXcreatedX_values() {
        final Traversal<Vertex, Map<String, List<String>>> traversal = get_g_v1_outXcreatedX_valueMap(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, List<String>> values = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals("lop", values.get("name").get(0));
        assertEquals("java", values.get("lang").get(0));
        assertEquals(2, values.size());
    }

    public static class JavaValueMapTest extends ValueMapTest {

        public JavaValueMapTest() {
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
        public Traversal<Edge, Map<String, Object>> get_g_E_valueMapXid_label_weightX() {
            return g.E().valueMap("id", "label", "weight");
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_v1_outXcreatedX_valueMap(final Object v1Id) {
            return g.v(v1Id).out("created").valueMap();
        }
    }

    public static class JavaComputerValueMapTest extends ValueMapTest {

        public JavaComputerValueMapTest() {
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
        public Traversal<Edge, Map<String, Object>> get_g_E_valueMapXid_label_weightX() {
            return g.E().valueMap("id", "label", "weight").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_v1_outXcreatedX_valueMap(final Object v1Id) {
            return (Traversal) g.v(v1Id).out("created").valueMap().submit(g.compute());
        }
    }
}
