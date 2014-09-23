package com.tinkerpop.gremlin.structure;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSystemTest {

    @Test
    public void shouldHandleSystemKeyManipulationCorrectly() {
        String key = "key";
        assertFalse(Graph.System.isSystem(key));
        assertTrue(Graph.System.isSystem(Graph.System.system(key)));
        assertEquals(Graph.System.system("").concat(key), Graph.System.system(key));
        for (int i = 0; i < 10; i++) {
            key = Graph.System.system(key);
        }
        assertTrue(Graph.System.isSystem(key));
        assertEquals(Graph.System.system("key"), key);
        assertEquals("key", Graph.System.unSystem(key));
        for (int i = 0; i < 10; i++) {
            key = Graph.System.unSystem(key);
        }
        assertFalse(Graph.System.isSystem(key));
        assertEquals("key", key);
    }
}
