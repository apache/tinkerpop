package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class InjectTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, String> get_g_VX1X_out_injectXv2X_name(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Path> get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_injectXv2X_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_out_injectXv2X_name(convertToVertexId("marko"), convertToVertexId("vadas"));
        printTraversalForm(traversal);
        Map<String, Long> counter = new HashMap<>();
        while (traversal.hasNext()) {
            MapHelper.incr(counter, traversal.next(), 1l);
        }
        assertEquals(3, counter.size());
        assertEquals(1l, counter.get("josh").longValue());
        assertEquals(1l, counter.get("lop").longValue());
        assertEquals(2l, counter.get("vadas").longValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            if (path.get("a").equals("daniel")) {
                assertEquals(2, path.size());
                assertEquals(6, (int) path.get(1));
            } else {
                assertEquals(4, path.size());
                assertEquals(path.<String>get("a").length(), (int) path.get(3));
            }
        }
        assertEquals(4, counter);
    }

    public static class StandardTest extends InjectTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_injectXv2X_name(final Object v1Id, final Object v2Id) {
            return g.V(v1Id).out().inject(g.V(v2Id).next()).values("name");
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(final Object v1Id) {
            return g.V(v1Id).out().<String>values("name").inject("daniel").as("a").map(t -> t.get().length()).path();
        }
    }

    public static class ComputerTest extends InjectTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_injectXv2X_name(final Object v1Id, final Object v2Id) {
            return g.V(v1Id).out().inject(g.V(v2Id).next()).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(final Object v1Id) {
            return g.V(v1Id).out().<String>values("name").inject("daniel").as("a").map(t -> t.get().length()).path().submit(g.compute());
        }
    }
}
