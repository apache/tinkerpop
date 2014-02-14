package com.tinkerpop.gremlin.process.steps.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import org.junit.Test;

import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class GroupCountTest extends AbstractGremlinTest {

    public abstract Map<Object, Long> get_g_V_outXcreatedX_groupCountXnameX();

    public abstract Map<Object, Long> get_g_V_outXcreatedX_name_groupCount();

    public abstract Map<Object, Long> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_outXcreatedX_groupCountXnameX() {
        final Map<Object, Long> map = get_g_V_outXcreatedX_groupCountXnameX();
        assertEquals(map.size(), 2);
        assertEquals(map.get("lop"), Long.valueOf(3l));
        assertEquals(map.get("ripple"), Long.valueOf(1l));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_outXcreatedX_name_groupCount() {
        final Map<Object, Long> map = get_g_V_outXcreatedX_name_groupCount();
        assertEquals(map.size(), 2);
        assertEquals(map.get("lop").longValue(), 3l);
        assertEquals(map.get("ripple").longValue(), 1l);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX() {
        final Map<Object, Long> map = get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX();
        assertEquals(map.size(), 4);
        assertEquals(map.get("lop").longValue(), 4l);
        assertEquals(map.get("ripple").longValue(), 2l);
        assertEquals(map.get("josh").longValue(), 1l);
        assertEquals(map.get("vadas").longValue(), 1l);
    }

    public static class JavaGroupCountTest extends GroupCountTest {
        public Map<Object, Long> get_g_V_outXcreatedX_groupCountXnameX() {
            return g.V().out("created").groupCount(v -> v.getValue("name"));
        }

        public Map<Object, Long> get_g_V_outXcreatedX_name_groupCount() {
            return g.V().out("created").value("name").groupCount();
        }

        public Map<Object, Long> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX() {
            return g.V().as("x").out()
                        .groupCount("a", v -> v.getValue("name"))
                        .jump("x", h -> h.getLoops() < 2).iterate().memory().get("a");
        }
    }
}