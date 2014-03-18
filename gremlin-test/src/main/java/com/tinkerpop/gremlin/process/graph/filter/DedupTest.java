package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class DedupTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, String> get_g_V_both_dedup_name();

    public abstract Traversal<Vertex, String> get_g_V_both_dedupXlangX_name();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_both_dedup_name() {
        final Iterator<String> traversal = get_g_V_both_dedup_name();
        System.out.println("Testing: " + traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(6, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("peter"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_both_dedupXlangX_name() {
        final Iterator<String> traversal = get_g_V_both_dedupXlangX_name();
        System.out.println("Testing: " + traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(2, names.size());
        assertTrue(names.contains("marko") || names.contains("peter") || names.contains("josh") || names.contains("vadas"));
        assertTrue(names.contains("lop") || names.contains("ripple"));
        assertFalse(traversal.hasNext());
    }

    public static class JavaDedupTest extends DedupTest {

        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            return g.V().both().dedup().value("name");
        }

        public Traversal<Vertex, String> get_g_V_both_dedupXlangX_name() {
            return g.V().both().dedup(v -> v.getProperty("lang").orElse(null)).value("name");
        }
    }
}
