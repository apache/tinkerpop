package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SideEffectCapTest extends AbstractGremlinTest {
    public abstract Traversal<Vertex, Map<Integer, Long>> get_g_V_hasXageX_groupCountXa_valueX_out_capXaX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXaX_outXcreatedX_inXcreatedX_linkBothXcocreator_aX() {
        final Iterator<Map<Integer, Long>> step = get_g_V_hasXageX_groupCountXa_valueX_out_capXaX();
        System.out.println("Testing: " + step);
        Map<Integer, Long> map = step.next();
        assertFalse(step.hasNext());
        assertEquals(map.get(27), new Long(1l));
        assertEquals(map.get(29), new Long(1l));
        assertEquals(map.get(32), new Long(1l));
        assertEquals(map.get(35), new Long(1l));
        assertEquals(map.size(), 4);
    }

    public static class JavaSideEffectCapTest extends SideEffectCapTest {

        public Traversal<Vertex, Map<Integer, Long>> get_g_V_hasXageX_groupCountXa_valueX_out_capXaX() {
            return g.V().<Vertex>has("age").groupCount("a", v -> v.value("age")).out().cap("a");
        }
    }

    public static class JavaComputerSideEffectCapTest extends SideEffectCapTest {
        public Traversal<Vertex, Map<Integer, Long>> get_g_V_hasXageX_groupCountXa_valueX_out_capXaX() {
            return (Traversal) g.V().<Vertex>has("age").groupCount("a", v -> v.value("age")).out().cap("a").submit(g.compute());
        }
    }
}
