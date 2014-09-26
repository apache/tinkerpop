package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.util.Tree;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class TreeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_treeXidX();

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_treeXa_idX();

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_treeXaX();

    public abstract Traversal<Vertex, Tree> get_g_v1_out_out_treeXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Tree> get_g_v1_out_out_treeXa_nameX_both_both_capXaX(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out_out_treeXnameX() {
        List<Traversal<Vertex, Tree>> traversals = Arrays.asList(
                get_g_v1_out_out_treeXnameX(convertToVertexId("marko")),
                get_g_v1_out_out_treeXa_nameX_both_both_capXaX(convertToVertexId("marko")));
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Tree tree = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(1, tree.size());
            assertTrue(tree.containsKey("marko"));
            assertEquals(1, ((Map) tree.get("marko")).size());
            assertTrue(((Map) tree.get("marko")).containsKey("josh"));
            assertTrue(((Map) ((Map) tree.get("marko")).get("josh")).containsKey("lop"));
            assertTrue(((Map) ((Map) tree.get("marko")).get("josh")).containsKey("ripple"));
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_treeXidX() {
        List<Traversal<Vertex, Tree>> traversals = Arrays.asList(get_g_V_out_out_treeXidX(), get_g_V_out_out_treeXa_idX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Tree tree = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(1, tree.size());
            assertTrue(tree.containsKey(convertToVertexId("marko")));
            assertEquals(1, ((Map) tree.get(convertToVertexId("marko"))).size());
            assertTrue(((Map) tree.get(convertToVertexId("marko"))).containsKey(convertToVertexId("josh")));
            assertTrue(((Map) ((Map) tree.get(convertToVertexId("marko"))).get(convertToVertexId("josh"))).containsKey(convertToVertexId("lop")));
            assertTrue(((Map) ((Map) tree.get(convertToVertexId("marko"))).get(convertToVertexId("josh"))).containsKey(convertToVertexId("ripple")));
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_treeXaX() {
        List<Traversal<Vertex, Tree>> traversals = Arrays.asList(get_g_V_out_out_treeXaX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Tree tree = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(1, tree.size());
            assertTrue(tree.containsKey(convertToVertex(g, "marko")));
            assertEquals(1, ((Map) tree.get(convertToVertex(g, "marko"))).size());
            assertTrue(((Map) tree.get(convertToVertex(g, "marko"))).containsKey(convertToVertex(g, "josh")));
            assertTrue(((Map) ((Map) tree.get(convertToVertex(g, "marko"))).get(convertToVertex(g, "josh"))).containsKey(convertToVertex(g, "lop")));
            assertTrue(((Map) ((Map) tree.get(convertToVertex(g, "marko"))).get(convertToVertex(g, "josh"))).containsKey(convertToVertex(g, "ripple")));
        });
    }

    public static class StandardTest extends TreeTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Tree> get_g_v1_out_out_treeXnameX(final Object v1Id) {
            return (Traversal) g.v(v1Id).out().out().tree(v -> ((Vertex) v).value("name"));
        }

        @Override
        public Traversal<Vertex, Tree> get_g_v1_out_out_treeXa_nameX_both_both_capXaX(final Object v1Id) {
            return g.v(v1Id).out().out().tree("a", v -> ((Vertex) v).value("name")).both().both().cap("a");
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXidX() {
            return (Traversal) g.V().out().out().tree(v -> ((Vertex) v).id());
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXa_idX() {
            return (Traversal) g.V().out().out().tree("a", v -> ((Vertex) v).id());
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX() {
            return (Traversal) g.V().out().out().tree("a");
        }
    }

    public static class ComputerTest extends TreeTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Tree> get_g_v1_out_out_treeXnameX(final Object v1Id) {
            // TODO: micropaths don't have vertex properties
            return (Traversal) g.v(v1Id).out().out().tree(v -> ((Vertex) v).value("name"));
        }

        @Override
        public Traversal<Vertex, Tree> get_g_v1_out_out_treeXa_nameX_both_both_capXaX(final Object v1Id) {
            // TODO: micropaths don't have vertex properties
            return g.v(v1Id).out().out().tree("a", v -> ((Vertex) v).value("name")).both().both().<Tree>cap("a");
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXidX() {
            return (Traversal) g.V().out().out().tree(v -> ((Vertex) v).id()).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXa_idX() {
            return (Traversal) g.V().out().out().tree("a", v -> ((Vertex) v).id()).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX() {
            return (Traversal) g.V().out().out().tree("a").submit(g.compute());
        }
    }
}
