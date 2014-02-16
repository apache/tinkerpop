package com.tinkerpop.gremlin.process.steps.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class TreeTest extends AbstractGremlinTest {
    public abstract Map get_g_v1_out_out_treeXnameX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_out_treeXnameX() {
        final Map map = get_g_v1_out_out_treeXnameX();
        assertEquals(1, map.size());
        assertTrue(map.containsKey("marko"));
        assertEquals(1, ((Map) map.get("marko")).size());
        assertTrue(((Map) map.get("marko")).containsKey("josh"));
        assertTrue(((Map) ((Map) map.get("marko")).get("josh")).containsKey("lop"));
        assertTrue(((Map) ((Map) map.get("marko")).get("josh")).containsKey("ripple"));
    }

    public static class JavaTreeTest extends TreeTest {
        public Map get_g_v1_out_out_treeXnameX() {
            return g.v(1).out().out().tree(v -> ((Vertex) v).getValue("name"));
        }
    }
}
