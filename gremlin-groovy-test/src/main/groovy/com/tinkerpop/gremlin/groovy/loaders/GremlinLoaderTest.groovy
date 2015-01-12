package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.AbstractGremlinTest
import org.junit.Test

import static org.junit.Assert.assertEquals

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinLoaderTest extends AbstractGremlinTest {
    @Test
    public void shouldCalculateMean() {
        assertEquals(25.0d, [10, 20, 30, 40].mean(), 0.00001d)
    }
}
