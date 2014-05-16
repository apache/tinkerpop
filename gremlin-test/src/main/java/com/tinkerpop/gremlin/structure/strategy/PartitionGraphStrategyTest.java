package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
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

		strategy.addReadPartition("A");
		assertEquals(2, g.V().count());
    }

	@Test
	@Ignore
	public void shouldWriteToMultiplePartitions() {
		final Vertex vA = g.addVertex("any", "a");
		final Vertex vAA = g.addVertex("any", "aa");
		final Edge vAvAA = vA.addEdge("a->a", vAA);

		final PartitionGraphStrategy strategy = (PartitionGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
		strategy.setWritePartition("B");
		final Vertex vB = g.addVertex("any", "b");
		vA.addEdge("a->b", vB);

		strategy.setWritePartition("C");
		final Vertex vC = g.addVertex("any", "c");
		vB.addEdge("b->c", vC);
		vA.addEdge("a->c", vC);

		final GraphTraversal t = g.V();
		assertTrue(t.optimizers().get().stream().anyMatch(o -> o.getClass().equals(PartitionGraphStrategy.PartitionGraphTraversalStrategy.class)));

		strategy.clearReadPartitions();
		assertEquals(0, g.V().count());
		assertEquals(0, g.E().count());

		strategy.addReadPartition("A");
		assertEquals(2, g.V().count());
		assertEquals(1, g.E().count());
		assertEquals(1, g.v(vA.id()).outE().count());
		assertEquals(vAvAA.id(), g.v(vA.id()).outE().next().id());
		assertEquals(1, g.v(vA.id()).out().count());
		assertEquals(vAA.id(), g.v(vA.id()).out().next().id());

		strategy.addReadPartition("B");
		assertEquals(3, g.V().count());
		assertEquals(2, g.E().count());

		strategy.addReadPartition("C");
		assertEquals(4, g.V().count());
		assertEquals(4, g.E().count());
	}
}
