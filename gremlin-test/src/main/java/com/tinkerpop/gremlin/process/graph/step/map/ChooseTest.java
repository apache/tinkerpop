package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ChooseTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_chooseXname_length_5XoutXinX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_chooseXname_length_5XoutXinX_name();
        System.out.println("Testing: " + traversal);
        Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(9, counter);
        assertEquals(5, counts.size());
        assertEquals(Long.valueOf(1), counts.get("vadas"));
        assertEquals(Long.valueOf(3), counts.get("josh"));
        assertEquals(Long.valueOf(2), counts.get("lop"));
        assertEquals(Long.valueOf(2), counts.get("marko"));
        assertEquals(Long.valueOf(1), counts.get("peter"));

    }

    public static class JavaChooseTest extends ChooseTest {

        public Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name() {
            return g.V().choose(t -> t.get().<String>value("name").length() == 5 ? 0 : 1, g.of().out(), g.of().in()).value("name");
        }
    }

    /*public static class JavaComputerIfThenElseTest extends IfThenElseTest {

        public Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name() {
            return (Traversal) g.V().map(t -> t.get().outE()).unfold().submit(g.compute());
        }
    }*/
}
