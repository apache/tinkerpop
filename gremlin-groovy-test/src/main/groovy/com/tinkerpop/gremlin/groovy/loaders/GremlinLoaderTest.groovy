package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.AbstractGremlinTest
import com.tinkerpop.gremlin.FeatureRequirement
import com.tinkerpop.gremlin.FeatureRequirements
import com.tinkerpop.gremlin.LoadGraphWith
import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.IoTest
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
