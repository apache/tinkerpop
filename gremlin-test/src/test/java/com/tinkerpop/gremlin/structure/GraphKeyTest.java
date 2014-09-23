package com.tinkerpop.gremlin.structure;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphKeyTest {

    @Test
    public void shouldHandleHiddenKeyManipulationCorrectly() {
        String key = "key";
        assertFalse(Graph.Key.isHidden(key));
        assertTrue(Graph.Key.isHidden(Graph.Key.hide(key)));
        assertEquals(Graph.Key.hide("").concat(key), Graph.Key.hide(key));
        for (int i = 0; i < 10; i++) {
            key = Graph.Key.hide(key);
        }
        assertTrue(Graph.Key.isHidden(key));
        assertEquals(Graph.Key.hide("key"), key);
        assertEquals("key", Graph.Key.unHide(key));
        for (int i = 0; i < 10; i++) {
            key = Graph.Key.unHide(key);
        }
        assertFalse(Graph.Key.isHidden(key));
        assertEquals("key", key);
    }
}
