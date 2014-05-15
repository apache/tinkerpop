package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionGraphStrategyTest extends AbstractGremlinTest {
    private static final String partition = Property.Key.hidden("partition");

    public PartitionGraphStrategyTest() {
        super(Optional.of(new PartitionGraphStrategy(partition, "A")));
    }

    @Test
    public void shouldAppendPartitionToVertex() {
        final Vertex v = g.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.property("any").get());
        assertEquals("A", v.property(partition).get());
    }

    @Test
    public void shouldAppendPartitionToEdge() {
        final Vertex v1 = g.addVertex("any", "thing");
        final Vertex v2 = g.addVertex("some", "thing");
        final Edge e = v1.addEdge("connectsTo", v2, "every", "thing");

        assertNotNull(v1);
        assertEquals("thing", v1.property("any").get());
        assertEquals("A", v2.property(partition).get());

        assertNotNull(v2);
        assertEquals("thing", v2.property("some").get());
        assertEquals("A", v2.property(partition).get());

        assertNotNull(e);
        assertEquals("thing", e.property("every").get());
        assertEquals("connectsTo", e.label());
        assertEquals("A", e.property(partition).get());
    }

    @Test
    public void shouldWriteVerticesToMultiplePartitions() {
        final Vertex vA = g.addVertex("any", "a");
        final PartitionGraphStrategy strategy = (PartitionGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
        strategy.setWritePartition("B");
        final Vertex vB = g.addVertex("any", "b");

        assertNotNull(vA);
        assertEquals("a", vA.property("any").get());
        assertEquals("A", vA.property(partition).get());

        assertNotNull(vB);
        assertEquals("b", vB.property("any").get());
        assertEquals("B", vB.property(partition).get());

        final GraphTraversal t = g.V();
        assertTrue(t.optimizers().get().stream().anyMatch(o -> o.getClass().equals(PartitionGraphStrategy.PartitionGraphTraversalOptimizer.class)));

        g.V().forEach(v -> {
            assertTrue(v instanceof StrategyWrappedVertex);
            assertEquals("a", v.property("any").get());
        });

        strategy.removeReadPartition("A");
        strategy.addReadPartition("B");

        g.V().forEach(v -> {
            assertTrue(v instanceof StrategyWrappedVertex);
            assertEquals("b", v.property("any").get());
        });
    }
}
