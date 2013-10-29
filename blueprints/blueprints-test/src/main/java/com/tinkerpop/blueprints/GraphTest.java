package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.GraphFactory;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GraphTest {
    /**
     * All Blueprints implementations should be constructable through GraphFactory.
     */
    @Test
    public void shouldOpenInMemoryGraphViaApacheConfig() {
        final Configuration conf = BlueprintsSuite.GraphManager.get().newGraphConfiguration();
        final Graph expectedGraph = BlueprintsSuite.GraphManager.get().newTestGraph();
        final Graph g = GraphFactory.open(conf);

        assertNotNull(g);
        assertEquals(expectedGraph.getClass(), g.getClass());
    }

    /**
     * Blueprints implementations should have private constructor as all graphs.  They should be only instantiated
     * through the GraphFactory or the static open() method on the Graph implementation itself.
     */
    @Test
    public void shouldHavePrivateConstructor() {
        final Graph expectedGraph = BlueprintsSuite.GraphManager.get().newTestGraph();
        assertTrue(Arrays.asList(expectedGraph.getClass().getConstructors()).stream().allMatch(c -> {
            final int modifier = c.getModifiers();
            return Modifier.isPrivate(modifier) || Modifier.isPrivate(modifier);
        }));
    }
}
