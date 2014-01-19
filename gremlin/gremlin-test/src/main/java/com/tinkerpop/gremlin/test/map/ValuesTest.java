package com.tinkerpop.gremlin.test.map;

import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValuesTest {
    public void testCompliance() {
        assertTrue(true);
    }

    public void g_V_values(final Iterator<Map<String, Object>> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            final Map<String, Object> values = pipe.next();
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

    public void g_V_valuesXname_ageX(final Iterator<Map<String, Object>> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            final Map<String, Object> values = pipe.next();
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

    public void g_E_valuesXid_label_weightX(final Iterator<Map<String, Object>> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            final Map<String, Object> values = pipe.next();
            final Integer id = Integer.valueOf(values.get("id").toString());
            if (id == 7) {
                assertEquals("knows", values.get("label"));
                assertEquals(0.5f, values.get("weight"));
                assertEquals(3, values.size());
            } else if (id == 8) {
                assertEquals("knows", values.get("label"));
                assertEquals(1.0f, values.get("weight"));
                assertEquals(3, values.size());
            } else if (id == 9) {
                assertEquals("created", values.get("label"));
                assertEquals(0.4f, values.get("weight"));
                assertEquals(3, values.size());
            } else if (id == 10) {
                assertEquals("created", values.get("label"));
                assertEquals(1.0f, values.get("weight"));
                assertEquals(3, values.size());
            } else if (id == 11) {
                assertEquals("created", values.get("label"));
                assertEquals(0.4f, values.get("weight"));
                assertEquals(3, values.size());
            } else if (id == 12) {
                assertEquals("created", values.get("label"));
                assertEquals(0.2f, values.get("weight"));
                assertEquals(3, values.size());
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
        }
        assertEquals(6, counter);
    }
}
