package com.tinkerpop.gremlin.process.steps.sideEffect;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TreeTest {

    public void g_v1_out_out_treeXnameX(final Map map) {
        assertEquals(1, map.size());
        assertTrue(map.containsKey("marko"));
        assertEquals(1, ((Map) map.get("marko")).size());
        assertTrue(((Map) map.get("marko")).containsKey("josh"));
        assertTrue(((Map) ((Map) map.get("marko")).get("josh")).containsKey("lop"));
        assertTrue(((Map) ((Map) map.get("marko")).get("josh")).containsKey("ripple"));
    }
}
