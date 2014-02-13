package com.tinkerpop.gremlin.process.steps.sideEffect;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupByTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_V_groupByXa_nameX(Map<String, List<Vertex>> map) {
        assertEquals(6, map.size());
        map.forEach((key, values) -> {
            assertEquals(1, values.size());
            if (key.equals("marko")) {
                assertEquals("1", values.get(0).getId());
            } else if (key.equals("vadas")) {
                assertEquals("2", values.get(0).getId());
            } else if (key.equals("lop")) {
                assertEquals("3", values.get(0).getId());
            } else if (key.equals("josh")) {
                assertEquals("4", values.get(0).getId());
            } else if (key.equals("ripple")) {
                assertEquals("5", values.get(0).getId());
            } else if (key.equals("peter")) {
                assertEquals("6", values.get(0).getId());
            } else {
                throw new IllegalStateException("This is not a possible return value");
            }
        });
    }

    public void g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX(Map<String, List<String>> map) {
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(2, map.get("java").size());
        assertTrue(map.get("java").contains("ripple"));
        assertTrue(map.get("java").contains("lop"));
    }

    public void g_V_hasXlangX_groupByXa_lang_1_countX(Map<String, Integer> map) {
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(Integer.valueOf(2), map.get("java"));
    }


}
