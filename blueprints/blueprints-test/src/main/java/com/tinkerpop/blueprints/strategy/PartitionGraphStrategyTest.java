package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionGraphStrategyTest extends AbstractBlueprintsTest {
    private static final String partition = Property.Key.hidden("partition");

    public PartitionGraphStrategyTest() {
        super(Optional.of(new PartitionGraphStrategy(partition, "A")));
    }

    @Test
    public void shouldAppendPartitionToVertex() {
        final Vertex v = g.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.getProperty("any").getValue());
        assertEquals("A", v.getProperty(partition).getValue());
    }

    @Test
    public void shouldAppendPartitionToEdge() {
        final Vertex v1 = g.addVertex("any", "thing");
        final Vertex v2 = g.addVertex("some", "thing");
        final Edge e = v1.addEdge("connectsTo", v2, "every", "thing");

        assertNotNull(v1);
        assertEquals("thing", v1.getProperty("any").getValue());
        assertEquals("A", v2.getProperty(partition).getValue());

        assertNotNull(v2);
        assertEquals("thing", v2.getProperty("some").getValue());
        assertEquals("A", v2.getProperty(partition).getValue());

        assertNotNull(e);
        assertEquals("thing", e.getProperty("every").getValue());
        assertEquals("connectsTo", e.getLabel());
        assertEquals("A", e.getProperty(partition).getValue());
    }

    @Test
    public void shouldWriteVerticesToMultiplePartitions() {
        final Vertex vA = g.addVertex("any", "a");
        final PartitionGraphStrategy strategy = (PartitionGraphStrategy) g.strategy().getGraphStrategy().get();
        strategy.setWritePartition("B");
        final Vertex vB = g.addVertex("any", "b");

        assertNotNull(vA);
        assertEquals("a", vA.getProperty("any").getValue());
        assertEquals("A", vA.getProperty(partition).getValue());

        assertNotNull(vB);
        assertEquals("b", vB.getProperty("any").getValue());
        assertEquals("B", vB.getProperty(partition).getValue());

        g.query().vertices().forEach(v->assertEquals("a", v.getProperty("any").getValue()));

        strategy.removeReadPartition("A");
        strategy.addReadPartition("B");

        g.query().vertices().forEach(v->assertEquals("b", v.getProperty("any").getValue()));
    }
}
