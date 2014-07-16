package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StoreTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Collection> get_g_V_storeXa_nameX_out_capXaX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_storeXa_nameX_out_capXaX() {
        final Iterator<Collection> step = get_g_V_storeXa_nameX_out_capXaX();
        Collection names = step.next();
        assertEquals(6, names.size());
        System.out.println(names);
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("vadas"));
        assertFalse(step.hasNext());
    }

    public static class JavaStoreTest extends StoreTest {
        public JavaStoreTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex, Collection> get_g_V_storeXa_nameX_out_capXaX() {
            return g.V().store("a", v -> v.value("name")).out().cap("a");
        }
    }

    public static class JavaComputerStoreTest extends StoreTest {
        public JavaComputerStoreTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex, Collection> get_g_V_storeXa_nameX_out_capXaX() {
            return g.V().store("a", v -> v.value("name")).out().<Collection>cap("a").submit(g.compute());
        }
    }

}
