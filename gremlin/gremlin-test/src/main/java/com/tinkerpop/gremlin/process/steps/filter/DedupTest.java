package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.AbstractToyGraphGremlinTest;
import com.tinkerpop.gremlin.structure.util.StreamFactory;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class DedupTest extends AbstractToyGraphGremlinTest {

    public abstract Iterator<String> get_g_V_both_dedup_name();

    public abstract Iterator<String> get_g_V_both_dedupXlangX_name();

    @Test
    public void g_V_both_dedup_name() {
        final Iterator<String> step = get_g_V_both_dedup_name();
        System.out.println("Testing: " + step);
        final List<String> names = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(6, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("peter"));
        assertFalse(step.hasNext());
    }

    @Test
    public void g_V_both_dedupXlangX_name() {
        final Iterator<String> step = get_g_V_both_dedupXlangX_name();
        System.out.println("Testing: " + step);
        final List<String> names = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(2, names.size());
        assertTrue(names.contains("marko") || names.contains("peter") || names.contains("josh") || names.contains("vadas"));
        assertTrue(names.contains("lop") || names.contains("ripple"));
        assertFalse(step.hasNext());
    }

    public static class JavaDedupTest extends DedupTest {

        public Iterator<String> get_g_V_both_dedup_name() {
            return g.V().both().dedup().value("name");
        }

        public Iterator<String> get_g_V_both_dedupXlangX_name() {
            return g.V().both().dedup(v -> v.getProperty("lang").orElse(null)).value("name");
        }
    }
}
