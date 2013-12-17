package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.GraphFactory;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;

import static org.junit.Assert.*;

public class GraphTest extends AbstractBlueprintsTest{
    /**
     * All Blueprints implementations should be constructable through GraphFactory.
     */
    @Test
    public void shouldOpenInMemoryGraphViaApacheConfig() {
        final Graph expectedGraph = g;
        final Graph createdGraph = GraphFactory.open(config);

        assertNotNull(createdGraph);
        assertEquals(expectedGraph.getClass(), createdGraph.getClass());
    }

    /**
     * Blueprints implementations should have private constructor as all graphs.  They should be only instantiated
     * through the GraphFactory or the static open() method on the Graph implementation itself.
     */
    @Test
    public void shouldHavePrivateConstructor() {
        assertTrue(Arrays.asList(g.getClass().getConstructors()).stream().allMatch(c -> {
            final int modifier = c.getModifiers();
            return Modifier.isPrivate(modifier) || Modifier.isPrivate(modifier);
        }));
    }
}
