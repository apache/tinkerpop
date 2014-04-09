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

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.junit.Assert.*;

/**
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public abstract class DedupRangeTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, String> get_g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupX0_2X_valueXnameX(Object vHunterId);

    public abstract Traversal<Vertex, String> get_g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupX3_5X_valueXnameX(Object vHunterId);

    public abstract Traversal<Vertex, String> get_g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupXname_split_0_0_5X_valueXnameX(Object vHunterId);

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupX0_2X_valueXnameX() {
        final Iterator<String> traversal = get_g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupX0_2X_valueXnameX(convertToId("Hunter"));
        System.out.println("Testing: " + traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(3, names.size());
        assertTrue(names.contains("Weir"));
        assertTrue(names.contains("Donna_Godchaux"));
        assertTrue(names.contains("Garcia"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupX3_5X_valueXnameX() {
        final Iterator<String> traversal = get_g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupX3_5X_valueXnameX(convertToId("Hunter"));
        System.out.println("Testing: " + traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(3, names.size());
        assertTrue(names.contains("Lesh"));
        assertTrue(names.contains("Weir_Hart"));
        assertTrue(names.contains("Garcia_Lesh"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupXname_split_0_0_5X_valueXnameX() {
        final Iterator<String> traversal = get_g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupXname_split_0_0_5X_valueXnameX(convertToId("Hunter"));
        System.out.println("Testing: " + traversal);
        final List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(5, names.size());
        assertTrue(names.contains("Weir"));
        assertTrue(names.contains("Donna_Godchaux"));
        assertTrue(names.contains("Garcia"));
        assertTrue(names.contains("Lesh"));
        assertTrue(names.contains("Pigpen_Weir"));
        assertFalse(traversal.hasNext());
    }

    public static class JavaDedupRangeTest extends DedupRangeTest {

        public Traversal<Vertex, String> get_g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupX0_2X_valueXnameX(final Object vHunterId) {
            return g.v(vHunterId).in("sung_by").out("followed_by").out("sung_by").dedup(0, 2).value("name");
        }

        public Traversal<Vertex, String> get_g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupX3_5X_valueXnameX(final Object vHunterId) {
            return g.v(vHunterId).in("sung_by").out("followed_by").out("sung_by").dedup(3, 5).value("name");
        }

        public Traversal<Vertex, String> get_g_v339_inXsung_byX_outXfollowed_byX_outXsung_byX_dedupXname_split_0_0_5X_valueXnameX(final Object vHunterId) {
            return g.v(vHunterId).in("sung_by").out("followed_by").out("sung_by").dedup(v ->
                    v.getProperty("name").get().toString().split("_")[0], 0, 5).value("name");
        }
    }
}
