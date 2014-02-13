package com.tinkerpop.gremlin.process.steps.sideEffect;

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

    public void g_V_outXcreatedX_groupCountXnameX(final Map<Object, Long> map) {
        assertEquals(map.size(), 2);
        assertEquals(map.get("lop"), Long.valueOf(3l));
        assertEquals(map.get("ripple"), Long.valueOf(1l));
    }

    public void g_V_outXcreatedX_name_groupCount(final Map<Object, Long> map) {
        assertEquals(map.size(), 2);
        assertEquals(map.get("lop").longValue(), 3l);
        assertEquals(map.get("ripple").longValue(), 1l);
    }

    public void g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX(final Map<Object, Long> map) {
        assertEquals(map.size(), 4);
        assertEquals(map.get("lop").longValue(), 4l);
        assertEquals(map.get("ripple").longValue(), 2l);
        assertEquals(map.get("josh").longValue(), 1l);
        assertEquals(map.get("vadas").longValue(), 1l);
    }
}