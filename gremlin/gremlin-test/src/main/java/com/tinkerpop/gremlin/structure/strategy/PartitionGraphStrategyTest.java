package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        assertEquals("thing", v.getProperty("any").get());
        assertEquals("A", v.getProperty(partition).get());
    }

    @Test
    public void shouldAppendPartitionToEdge() {
        final Vertex v1 = g.addVertex("any", "thing");
        final Vertex v2 = g.addVertex("some", "thing");
        final Edge e = v1.addEdge("connectsTo", v2, "every", "thing");

        assertNotNull(v1);
        assertEquals("thing", v1.getProperty("any").get());
        assertEquals("A", v2.getProperty(partition).get());

        assertNotNull(v2);
        assertEquals("thing", v2.getProperty("some").get());
        assertEquals("A", v2.getProperty(partition).get());

        assertNotNull(e);
        assertEquals("thing", e.getProperty("every").get());
        assertEquals("connectsTo", e.getLabel());
        assertEquals("A", e.getProperty(partition).get());
    }

    @Ignore
    public void shouldWriteVerticesToMultiplePartitions() {
        final Vertex vA = g.addVertex("any", "a");
        final PartitionGraphStrategy strategy = (PartitionGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
        strategy.setWritePartition("B");
        final Vertex vB = g.addVertex("any", "b");

        assertNotNull(vA);
        assertEquals("a", vA.getProperty("any").get());
        assertEquals("A", vA.getProperty(partition).get());

        assertNotNull(vB);
        assertEquals("b", vB.getProperty("any").get());
        assertEquals("B", vB.getProperty(partition).get());

        g.V().forEach(v -> assertEquals("a", v.getProperty("any").get()));

        strategy.removeReadPartition("A");
        strategy.addReadPartition("B");

        g.V().forEach(v -> assertEquals("b", v.getProperty("any").get()));
    }
}
