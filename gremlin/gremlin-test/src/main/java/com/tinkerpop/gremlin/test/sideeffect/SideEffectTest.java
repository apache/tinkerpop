package com.tinkerpop.gremlin.test.sideeffect;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_v1_sideEffectXstore_aX_valueXnameX(final Iterator<String> pipe) {
        assertEquals(pipe.next(), "marko");
        assertFalse(pipe.hasNext());
    }

    public void g_v1_out_sideEffectXincr_cX_valueXnameX(final Iterator<String> pipe) {
        List<String> names = new ArrayList<String>();
        while (pipe.hasNext()) {
            names.add(pipe.next());
        }
        assertEquals(3, names.size());
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("vadas"));
    }

    public void g_v1_out_sideEffectXX_valueXnameX(final Iterator<String> pipe) {
        this.g_v1_out_sideEffectXincr_cX_valueXnameX(pipe);
    }
}
