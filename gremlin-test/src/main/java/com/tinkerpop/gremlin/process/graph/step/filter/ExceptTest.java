package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public abstract class ExceptTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_exceptXg_VX();

    public abstract Traversal<Vertex, Vertex> get_g_V_exceptXX();

    public abstract Traversal<Vertex, Path> get_g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_exceptXg_v2X() {
        final Iterator<Vertex> traversal = get_g_v1_out_exceptXg_v2X(convertToVertexId("marko"), convertToVertexId("vadas"));
        System.out.println("Testing: " + traversal);
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
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_aggregateXxX_out_exceptXxX() {
        Iterator<Vertex> traversal = get_g_v1_out_aggregateXxX_out_exceptXxX(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        assertEquals("ripple", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX() {
        Iterator<String> traversal = get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        List<String> names = Arrays.asList(traversal.next(), traversal.next());
        assertFalse(traversal.hasNext());
        assertEquals(2, names.size());
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("josh"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_exceptXg_VX() {
        Iterator<Vertex> traversal = get_g_V_exceptXg_VX();
        System.out.println("Testing: " + traversal);
        final List<Vertex> vertices = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(0, vertices.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_exceptXX() {
        Iterator<Vertex> traversal = get_g_V_exceptXX();
        System.out.println("Testing: " + traversal);
        final List<Vertex> vertices = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(6, vertices.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path() {
        Iterator<Path> traversal = get_g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        final List<Path> paths = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(4, paths.size());
        assertEquals(1, paths.stream().filter(path -> path.size() == 3).count());
        assertEquals(2, paths.stream().filter(path -> path.size() == 5).count());
        assertEquals(1, paths.stream().filter(path -> path.size() == 7).count());
        assertFalse(traversal.hasNext());
    }

    public static class JavaExceptTest extends ExceptTest {
        public Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
            return g.v(v1Id).out().except(g.v(v2Id));
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
            return g.v(v1Id).out().aggregate("x").out().except("x");
        }

        public Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").except(g.v(v1Id)).value("name");
        }

        public Traversal<Vertex, Vertex> get_g_V_exceptXg_VX() {
            return g.V().except(g.V().toList());
        }

        public Traversal<Vertex, Vertex> get_g_V_exceptXX() {
            return g.V().except(Collections.emptyList());
        }

        public Traversal<Vertex, Path> get_g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path(final Object v1Id) {
            return g.v(v1Id).as("x").bothE("created").except("e").aggregate("e").otherV().jump("x", x -> true, x -> true).path();
        }
    }

    public static class JavaComputerExceptTest extends ExceptTest {
        public Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
            return g.v(v1Id).out().except(g.v(v2Id)).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
            return g.v(v1Id).out().aggregate("x").out().except("x").submit(g.compute());
        }

        public Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").except(g.v(v1Id)).<String>value("name").submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_exceptXg_VX() {
            return g.V().except(g.V().toList()).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_exceptXX() {
            return g.V().except(Collections.emptyList()).submit(g.compute());
        }

        public Traversal<Vertex, Path> get_g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path(final Object v1Id) {
            return g.v(v1Id).as("x").bothE("created").except("e").aggregate("e").otherV().jump("x", x -> true, x -> true).path().submit(g.compute());
        }
    }
}
