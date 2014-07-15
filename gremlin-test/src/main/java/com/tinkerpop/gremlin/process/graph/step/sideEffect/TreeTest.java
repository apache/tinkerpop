package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.util.Tree;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class TreeTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Tree> get_g_v1_out_out_treeXnameX(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_out_treeXnameX() {
        final Iterator<Tree> step = get_g_v1_out_out_treeXnameX(convertToVertexId("marko"));
        final Tree tree = step.next();
        assertFalse(step.hasNext());
        assertEquals(1, tree.size());
        assertTrue(tree.containsKey("marko"));
        assertEquals(1, ((Map) tree.get("marko")).size());
        assertTrue(((Map) tree.get("marko")).containsKey("josh"));
        assertTrue(((Map) ((Map) tree.get("marko")).get("josh")).containsKey("lop"));
        assertTrue(((Map) ((Map) tree.get("marko")).get("josh")).containsKey("ripple"));
    }

    public static class JavaTreeTest extends TreeTest {
        public JavaTreeTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex, Tree> get_g_v1_out_out_treeXnameX(final Object v1Id) {
            return (Traversal) g.v(v1Id).out().out().tree(v -> ((Vertex) v).value("name"));
        }
    }

    public static class JavaComputerTreeTest extends TreeTest {
        public JavaComputerTreeTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex, Tree> get_g_v1_out_out_treeXnameX(final Object v1Id) {
            // todo: micropaths don't have vertex properties
            return (Traversal) g.v(v1Id).out().out().tree(v -> ((Vertex) v).value("name"));
        }
    }
}
