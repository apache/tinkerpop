package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionGraphStrategyTest extends AbstractGremlinTest {
    private static final String partition = Property.hidden("partition");

    public PartitionGraphStrategyTest() {
        super(Optional.of((GraphStrategy) new PartitionGraphStrategy(partition, "A")));
    }

    @Test
    public void shouldAppendPartitionToVertex() {
        final Vertex v = g.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals("A", v.property(partition).value());
    }

    @Test
    public void shouldAppendPartitionToEdge() {
        final Vertex v1 = g.addVertex("any", "thing");
        final Vertex v2 = g.addVertex("some", "thing");
        final Edge e = v1.addEdge("connectsTo", v2, "every", "thing");

        assertNotNull(v1);
        assertEquals("thing", v1.property("any").value());
        assertEquals("A", v2.property(partition).value());

        assertNotNull(v2);
        assertEquals("thing", v2.property("some").value());
        assertEquals("A", v2.property(partition).value());

        assertNotNull(e);
        assertEquals("thing", e.property("every").value());
        assertEquals("connectsTo", e.label());
        assertEquals("A", e.property(partition).value());
    }

    @Test
    public void shouldWriteVerticesToMultiplePartitions() {
        final Vertex vA = g.addVertex("any", "a");
        final PartitionGraphStrategy strategy = (PartitionGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
        strategy.setWritePartition("B");
        final Vertex vB = g.addVertex("any", "b");

        assertNotNull(vA);
        assertEquals("a", vA.property("any").value());
        assertEquals("A", vA.property(partition).value());

        assertNotNull(vB);
        assertEquals("b", vB.property("any").value());
        assertEquals("B", vB.property(partition).value());

        final GraphTraversal t = g.V();
        assertTrue(t.optimizers().get().stream().anyMatch(o -> o.getClass().equals(PartitionGraphStrategy.PartitionGraphTraversalStrategy.class)));

        g.V().forEach(v -> {
            assertTrue(v instanceof StrategyWrappedVertex);
            assertEquals("a", v.property("any").value());
        });

        strategy.removeReadPartition("A");
        strategy.addReadPartition("B");

        g.V().forEach(v -> {
            assertTrue(v instanceof StrategyWrappedVertex);
            assertEquals("b", v.property("any").value());
        });
    }
}
