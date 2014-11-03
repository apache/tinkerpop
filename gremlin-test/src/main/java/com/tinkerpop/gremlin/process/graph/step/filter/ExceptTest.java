package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public abstract class ExceptTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_name(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_exceptXg_V_toListX();

    public abstract Traversal<Vertex, Vertex> get_g_V_exceptXX();

    public abstract Traversal<Vertex, Path> get_g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out_exceptXg_v2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_out_exceptXg_v2X(convertToVertexId("marko"), convertToVertexId("vadas"));
        printTraversalForm(traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("josh") || vertex.value("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out_aggregateXxX_out_exceptXxX() {
        Traversal<Vertex, Vertex> traversal = get_g_v1_out_aggregateXxX_out_exceptXxX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals("ripple", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX() {
        Traversal<Vertex, String> traversal = get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        List<String> names = Arrays.asList(traversal.next(), traversal.next());
        assertFalse(traversal.hasNext());
        assertEquals(2, names.size());
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("josh"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_exceptXg_V_toListX() {
        Traversal<Vertex, Vertex> traversal = get_g_V_exceptXg_V_toListX();
        printTraversalForm(traversal);
        final List<Vertex> vertices = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(0, vertices.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_exceptXX() {
        Traversal<Vertex, Vertex> traversal = get_g_V_exceptXX();
        printTraversalForm(traversal);
        final List<Vertex> vertices = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(6, vertices.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path() {
        Traversal<Vertex, Path> traversal = get_g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<Path> paths = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(4, paths.size());
        assertEquals(1, paths.stream().filter(path -> path.size() == 3).count());
        assertEquals(2, paths.stream().filter(path -> path.size() == 5).count());
        assertEquals(1, paths.stream().filter(path -> path.size() == 7).count());
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends ExceptTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
            return g.v(v1Id).out().except(g.v(v2Id));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
            return g.v(v1Id).out().aggregate("x").out().except("x");
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_name(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").except(g.v(v1Id)).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXg_V_toListX() {
            return g.V().except(g.V().toList());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXX() {
            return g.V().except(Collections.emptyList());
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path(final Object v1Id) {
            return g.v(v1Id).as("x").bothE("created").except("e").aggregate("e").otherV().jump("x", x -> true, x -> true).path();
        }
    }

    public static class ComputerTest extends ExceptTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
            return g.v(v1Id).out().except(g.v(v2Id)).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
            return g.v(v1Id).out().aggregate("x").out().except("x").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_name(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").except(g.v(v1Id)).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXg_V_toListX() {
            return g.V().except(g.V().toList()).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXX() {
            return g.V().except(Collections.emptyList()).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path(final Object v1Id) {
            return g.v(v1Id).as("x").bothE("created").except("e").aggregate("e").otherV().jump("x", x -> true, x -> true).path().submit(g.compute());
        }
    }
}
