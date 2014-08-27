package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Collection;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StoreTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Collection> get_g_V_storeXa_nameX_out_capXaX();

    public abstract Traversal<Vertex, Collection> get_g_v1_storeXa_nameX_out_storeXa_nameX_name_capXaX(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_storeXa_nameX_out_capXaX() {
        final Traversal<Vertex, Collection> traversal = get_g_V_storeXa_nameX_out_capXaX();
        printTraversalForm(traversal);
        Collection names = traversal.next();
        assertEquals(6, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("vadas"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_storeXa_nameX_out_storeXa_nameX_name_capXaX() {
        final Traversal<Vertex, Collection> traversal = get_g_v1_storeXa_nameX_out_storeXa_nameX_name_capXaX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        Collection names = traversal.next();
        assertEquals(4, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("lop"));
        assertFalse(traversal.hasNext());
    }

    public static class JavaStoreTest extends StoreTest {
        public JavaStoreTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Collection> get_g_V_storeXa_nameX_out_capXaX() {
            return g.V().store("a", v -> v.get().value("name")).out().cap("a");
        }

        @Override
        public Traversal<Vertex, Collection> get_g_v1_storeXa_nameX_out_storeXa_nameX_name_capXaX(final Object v1Id) {
            return g.v(v1Id).store("a", v -> v.get().value("name")).out().store("a", v -> v.get().value("name")).value("name").cap("a");
        }
    }

    public static class JavaComputerStoreTest extends StoreTest {
        public JavaComputerStoreTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Collection> get_g_V_storeXa_nameX_out_capXaX() {
            return g.V().store("a", v -> v.get().value("name")).out().<Collection>cap("a").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Collection> get_g_v1_storeXa_nameX_out_storeXa_nameX_name_capXaX(final Object v1Id) {
            return g.v(v1Id).store("a", v -> v.get().value("name")).out().store("a", v -> v.get().value("name")).value("name").<Collection>cap("a").submit(g.compute());
        }
    }

}
