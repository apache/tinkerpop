package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.GraphFactory;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphTest extends AbstractBlueprintsTest {

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

    /**
     * Test graph counts with addition and removal of vertices.
     */
    @Test
    public void shouldCountVerticesAndEdgesInTheGraph() {
        BlueprintsSuite.assertVertexEdgeCounts(g, 0, 0);
        final Vertex v = g.addVertex();
        BlueprintsSuite.assertVertexEdgeCounts(g, 1, 0);
        assertEquals(v, g.query().vertices().iterator().next());
        assertEquals(v.getId(), g.query().vertices().iterator().next().getId());
        assertEquals(v.getLabel(), g.query().vertices().iterator().next().getLabel());
        v.remove();
        BlueprintsSuite.assertVertexEdgeCounts(g, 0, 0);
        g.addVertex();
        g.addVertex();
        BlueprintsSuite.assertVertexEdgeCounts(g, 2, 0);
        g.query().vertices().forEach(Vertex::remove);
        BlueprintsSuite.assertVertexEdgeCounts(g, 0, 0);
    }
}
