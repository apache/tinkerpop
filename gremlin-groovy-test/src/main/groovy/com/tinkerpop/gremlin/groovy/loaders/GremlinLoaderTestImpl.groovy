package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.structure.Graph
import org.junit.Test

import static org.junit.Assert.assertEquals

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinLoaderTestImpl {

    @Test
    public void shouldHideAndUnhideKeys() {
        assertEquals(Graph.Key.hide("g"), -"g");
        assertEquals("g", -Graph.Key.hide("g"));
        assertEquals("g", -(-"g"));
    }
}
