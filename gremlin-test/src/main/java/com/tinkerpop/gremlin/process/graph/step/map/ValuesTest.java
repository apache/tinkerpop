package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class ValuesTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_values();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_valuesXname_ageX();

    public abstract Traversal<Edge, Map<String, Object>> get_g_E_valuesXid_label_weightX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_v1_outXcreatedX_values(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_values() {
        final Iterator<Map<String, Object>> step = get_g_V_values();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            final Map<String, Object> values = step.next();
            final String name = (String) values.get("name");
            assertEquals(2, values.size());
            if (name.equals("marko")) {
                assertEquals(29, values.get("age"));
            } else if (name.equals("josh")) {
                assertEquals(32, values.get("age"));
            } else if (name.equals("peter")) {
                assertEquals(35, values.get("age"));
            } else if (name.equals("vadas")) {
                assertEquals(27, values.get("age"));
            } else if (name.equals("lop")) {
                assertEquals("java", values.get("lang"));
            } else if (name.equals("ripple")) {
                assertEquals("java", values.get("lang"));
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_valuesXname_ageX() {
        final Iterator<Map<String, Object>> step = get_g_V_valuesXname_ageX();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            final Map<String, Object> values = step.next();
            final String name = (String) values.get("name");
            if (name.equals("marko")) {
                assertEquals(29, values.get("age"));
                assertEquals(2, values.size());
            } else if (name.equals("josh")) {
                assertEquals(32, values.get("age"));
                assertEquals(2, values.size());
            } else if (name.equals("peter")) {
                assertEquals(35, values.get("age"));
                assertEquals(2, values.size());
            } else if (name.equals("vadas")) {
                assertEquals(27, values.get("age"));
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
    @LoadGraphWith(CLASSIC)
    public void g_E_valuesXid_label_weightX() {
        final Iterator<Map<String, Object>> step = get_g_E_valuesXid_label_weightX();
        System.out.println("Testing: " + step);
        int counter = 0;
        int counter2 = 0;
        while (step.hasNext()) {
            counter++;
            final Map<String, Object> values = step.next();
            if (values.get("label").equals("knows") && values.get("weight").equals(0.5f) && values.size() == 3)
                counter2++;
            else if (values.get("label").equals("knows") && values.get("weight").equals(1.0f) && values.size() == 3)
                counter2++;
            else if (values.get("label").equals("created") && values.get("weight").equals(0.4f) && values.size() == 3)
                counter2++;
            else if (values.get("label").equals("created") && values.get("weight").equals(1.0f) && values.size() == 3)
                counter2++;
            else if (values.get("label").equals("created") && values.get("weight").equals(0.4f) && values.size() == 3)
                counter2++;
            else if (values.get("label").equals("created") && values.get("weight").equals(0.2f) && values.size() == 3)
                counter2++;
        }
        assertEquals(6, counter2);
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXcreatedX_values() {
        final Iterator<Map<String, Object>> step = get_g_v1_outXcreatedX_values(convertToVertexId("marko"));
        System.out.println("Testing: " + step);
        assertTrue(step.hasNext());
        final Map<String, Object> values = step.next();
        assertFalse(step.hasNext());
        assertEquals("lop", values.get("name"));
        assertEquals("java", values.get("lang"));
        assertEquals(2, values.size());
    }

    public static class JavaValuesTest extends ValuesTest {

        public JavaValuesTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex, Map<String, Object>> get_g_V_values() {
            return g.V().values();
        }

        public Traversal<Vertex, Map<String, Object>> get_g_V_valuesXname_ageX() {
            return g.V().values("name", "age");
        }

        public Traversal<Edge, Map<String, Object>> get_g_E_valuesXid_label_weightX() {
            return g.E().values("id", "label", "weight");
        }

        public Traversal<Vertex, Map<String, Object>> get_g_v1_outXcreatedX_values(final Object v1Id) {
            return g.v(v1Id).out("created").values();
        }
    }

    public static class JavaComputerValuesTest extends ValuesTest {

        public JavaComputerValuesTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex, Map<String, Object>> get_g_V_values() {
            return g.V().values().submit(g.compute());
        }

        public Traversal<Vertex, Map<String, Object>> get_g_V_valuesXname_ageX() {
            return g.V().values("name", "age").submit(g.compute());
        }

        public Traversal<Edge, Map<String, Object>> get_g_E_valuesXid_label_weightX() {
            return g.E().values("id", "label", "weight").submit(g.compute());
        }

        public Traversal<Vertex, Map<String, Object>> get_g_v1_outXcreatedX_values(final Object v1Id) {
            return g.v(v1Id).out("created").values().submit(g.compute());
        }
    }
}
