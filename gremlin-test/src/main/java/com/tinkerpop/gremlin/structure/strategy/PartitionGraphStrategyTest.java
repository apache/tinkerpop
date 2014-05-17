package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
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
        assertTrue(t.strategies().get().stream().anyMatch(o -> o.getClass().equals(PartitionGraphStrategy.PartitionGraphTraversalStrategy.class)));

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
    public void shouldThrowExceptionOnvInDifferentPartition() {
        final Vertex vA = g.addVertex("any", "a");
        assertEquals(vA.id(), g.v(vA.id()).id());

        final PartitionGraphStrategy strategy = (PartitionGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
        strategy.clearReadPartitions();

        try {
            g.v(vA.id());
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.elementNotFound();
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionOneInDifferentPartition() {
        final Vertex vA = g.addVertex("any", "a");
        final Edge e = vA.addEdge("knows", vA);
        assertEquals(e.id(), g.e(e.id()).id());

        final PartitionGraphStrategy strategy = (PartitionGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
        strategy.clearReadPartitions();

        try {
            g.e(e.id());
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.elementNotFound();
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
	public void shouldWriteToMultiplePartitions() {
		final Vertex vA = g.addVertex("any", "a");
		final Vertex vAA = g.addVertex("any", "aa");
		final Edge eAtoAA = vA.addEdge("a->a", vAA);

		final PartitionGraphStrategy strategy = (PartitionGraphStrategy) ((StrategyWrappedGraph) g).strategy().getGraphStrategy().get();
		strategy.setWritePartition("B");
		final Vertex vB = g.addVertex("any", "b");
		vA.addEdge("a->b", vB);

		strategy.setWritePartition("C");
		final Vertex vC = g.addVertex("any", "c");
		final Edge eBtovC = vB.addEdge("b->c", vC);
        final Edge eAtovC = vA.addEdge("a->c", vC);

		final GraphTraversal t = g.V();
		assertTrue(t.strategies().get().stream().anyMatch(o -> o.getClass().equals(PartitionGraphStrategy.PartitionGraphTraversalStrategy.class)));

		strategy.clearReadPartitions();
		assertEquals(0, g.V().count());
		assertEquals(0, g.E().count());

		strategy.addReadPartition("A");
		assertEquals(2, g.V().count());
		assertEquals(1, g.E().count());
		assertEquals(1, g.v(vA.id()).outE().count());
		assertEquals(eAtoAA.id(), g.v(vA.id()).outE().next().id());
		assertEquals(1, g.v(vA.id()).out().count());
		assertEquals(vAA.id(), g.v(vA.id()).out().next().id());

		strategy.addReadPartition("B");
		assertEquals(3, g.V().count());
		assertEquals(2, g.E().count());

		strategy.addReadPartition("C");
		assertEquals(4, g.V().count());
		assertEquals(4, g.E().count());

        strategy.removeReadPartition("A");
        strategy.removeReadPartition("B");

        assertEquals(1, g.V().count());
        assertEquals(2, g.E().count());

        assertEquals(2, g.v(vC.id()).inE().count());
        assertEquals(0, g.v(vC.id()).in().count());

        strategy.addReadPartition("B");
        assertEquals(2, g.v(vC.id()).inE().count());
        assertEquals(1, g.v(vC.id()).in().count());
        assertEquals(vC.id(), g.e(eBtovC.id()).inV().id().next());
        assertEquals(vB.id(), g.e(eBtovC.id()).outV().id().next());
        assertEquals(vC.id(), g.e(eAtovC.id()).inV().id().next());
        assertFalse(g.e(eAtovC.id()).outV().hasNext());

        strategy.addReadPartition("A");
        g.v(vA.id()).out().out().forEach(v -> {
            assertTrue(v instanceof StrategyWrapped);
            assertFalse(((StrategyWrappedElement) v).getBaseElement() instanceof StrategyWrapped);
        });

        g.v(vA.id()).outE().inV().outE().forEach(e -> {
            assertTrue(e instanceof StrategyWrapped);
            assertFalse(((StrategyWrappedElement) e).getBaseElement() instanceof StrategyWrapped);
        });
    }

    // todo: make sure all properties are wrapped
}
