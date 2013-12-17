package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.GraphFactory;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
     * Ensure compliance with Features by checking that all Features are exposed by the implementation.
     */
    @Test
    public void shouldImplementAndExposeFeatures() {
        final Graph.Features features = g.getFeatures();
        assertNotNull(features);

        final AtomicInteger counter = new AtomicInteger(0);

        // get all features.
        final List<Method> methods = Arrays.asList(Graph.Features.class.getMethods()).stream()
                .filter(m -> m.getReturnType().equals(Graph.Features.FeatureSet.class))
                .collect(Collectors.toList());

        methods.forEach(m -> {
                    try {
                        assertNotNull(m.invoke(features));
                        counter.incrementAndGet();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        fail("Exception while dynamically checking compliance on Feature implementation");
                    }
                });

        // always should be some feature methods
        assertTrue(methods.size() > 0);

        // ensure that every method exposed was checked
        assertEquals(methods.size(), counter.get());
    }

    @Test
    public void shouldConstructAnEmptyGraph() {
        BlueprintsSuite.assertVertexEdgeCounts(g, 0, 0);
    }

    /**
     * Test graph counts with addition and removal of vertices.
     */
    @Test
    public void shouldCountVerticesAndEdgesInTheGraph() {
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
