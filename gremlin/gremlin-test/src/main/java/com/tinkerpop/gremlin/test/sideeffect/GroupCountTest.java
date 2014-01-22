package com.tinkerpop.gremlin.test.sideeffect;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountTest {

    public void testCompliance() {
        assertTrue(true);
    }

    /*public void test_g_V_outXcreatedX_groupCountXm__nameX(Iterator<Vertex> pipe, Map<String, Number> m) {
        int counter = 0;
        while (pipe.hasNext()) {
            Vertex vertex = pipe.next();
            counter++;
            assertTrue(vertex.getValue("name").equals("lop") || vertex.getValue("name").equals("ripple"));
        }
        assertEquals(4, counter);
        assertEquals(2, m.size());
        assertEquals(3l, m.get("lop"));
        assertEquals(1l, m.get("ripple"));
    }*/

    public void g_V_outXcreatedX_name_groupCount(final Map<Object, Long> map) {
        assertEquals(map.size(), 2);
        assertEquals(map.get("lop").longValue(), 3l);
        assertEquals(map.get("ripple").longValue(), 1l);
    }
}
