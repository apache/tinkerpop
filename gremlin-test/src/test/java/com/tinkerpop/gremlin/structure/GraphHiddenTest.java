package com.tinkerpop.gremlin.structure;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphHiddenTest {

    @Test
    public void shouldHandleSystemKeyManipulationCorrectly() {
        String key = "key";
        assertFalse(Graph.Hidden.isHidden(key));
        assertTrue(Graph.Hidden.isHidden(Graph.Hidden.hide(key)));
        assertEquals(Graph.Hidden.hide("").concat(key), Graph.Hidden.hide(key));
        for (int i = 0; i < 10; i++) {
            key = Graph.Hidden.hide(key);
        }
        assertTrue(Graph.Hidden.isHidden(key));
        assertEquals(Graph.Hidden.hide("key"), key);
        assertEquals("key", Graph.Hidden.unHide(key));
        for (int i = 0; i < 10; i++) {
            key = Graph.Hidden.unHide(key);
        }
        assertFalse(Graph.Hidden.isHidden(key));
        assertEquals("key", key);
    }
}
