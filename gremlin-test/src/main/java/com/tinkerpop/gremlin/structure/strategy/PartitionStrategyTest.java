package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionStrategyTest extends AbstractGremlinTest {
    private static final String partition = "gremlin.partitionGraphStrategy.partition";

    public PartitionStrategyTest() {
        super(PartitionStrategy.build().partitionKey(partition).startPartition("A").create());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldAppendPartitionToVertex() {
        final Vertex v = g.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals("A", ((StrategyVertex) v).getBaseVertex().property(partition).value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldHidePartitionKeyOnVertex() {
        final Vertex v = g.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertFalse(v.property(partition).isPresent());
        assertFalse(v.iterators().propertyIterator(partition).hasNext());
        assertFalse(v.iterators().valueIterator(partition).hasNext());
        assertFalse(v.keys().contains(partition));
        assertEquals("A", ((StrategyVertex) v).getBaseVertex().property(partition).value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldAppendPartitionToEdge() {
        final Vertex v1 = g.addVertex("any", "thing");
        final Vertex v2 = g.addVertex("some", "thing");
        final Edge e = v1.addEdge("connectsTo", v2, "every", "thing");

        assertNotNull(v1);
        assertEquals("thing", v1.property("any").value());
        assertEquals("A", ((StrategyVertex) v2).getBaseVertex().property(partition).value());

        assertNotNull(v2);
        assertEquals("thing", v2.property("some").value());
        assertEquals("A", ((StrategyVertex) v2).getBaseVertex().property(partition).value());

        assertNotNull(e);
        assertEquals("thing", e.property("every").value());
        assertEquals("connectsTo", e.label());
        assertEquals("A", ((StrategyEdge) e).getBaseEdge().property(partition).value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldHidePartitionKeyOnEdge() {
        final Vertex v1 = g.addVertex("any", "thing");
        final Vertex v2 = g.addVertex("some", "thing");
        final Edge e = v1.addEdge("connectsTo", v2, "every", "thing");

        assertNotNull(e);
        assertEquals("thing", e.property("every").value());
        assertEquals("connectsTo", e.label());
        assertFalse(e.property(partition).isPresent());
        assertFalse(e.iterators().propertyIterator(partition).hasNext());
        assertFalse(e.iterators().valueIterator(partition).hasNext());
        assertFalse(e.keys().contains(partition));
        assertEquals("A", ((StrategyEdge) e).getBaseEdge().property(partition).value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldWriteVerticesToMultiplePartitions() {
        final Vertex vA = g.addVertex("any", "a");
        final PartitionStrategy strategy = (PartitionStrategy) ((StrategyGraph) g).getStrategy();
        strategy.setWritePartition("B");
        final Vertex vB = g.addVertex("any", "b");

        assertNotNull(vA);
        assertEquals("a", vA.property("any").value());
        assertEquals("A", ((StrategyVertex) vA).getBaseVertex().property(partition).value());

        assertNotNull(vB);
        assertEquals("b", vB.property("any").value());
        assertEquals("B", ((StrategyVertex) vB).getBaseVertex().property(partition).value());

        /* not applicable to SubgraphStrategy
        final GraphTraversal t = g.V();
        assertTrue(t.strategies().get().stream().anyMatch(o -> o.getClass().equals(PartitionGraphStrategy.PartitionGraphTraversalStrategy.class)));
        */

        g.V().forEachRemaining(v -> {
            assertTrue(v instanceof StrategyVertex);
            assertEquals("a", v.property("any").value());
        });

        strategy.removeReadPartition("A");
        strategy.addReadPartition("B");

        g.V().forEachRemaining(v -> {
            assertTrue(v instanceof StrategyVertex);
            assertEquals("b", v.property("any").value());
        });

        strategy.addReadPartition("A");
        assertEquals(new Long(2), g.V().count().next());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldThrowExceptionOnvInDifferentPartition() {
        final Vertex vA = g.addVertex("any", "a");
        assertEquals(vA.id(), g.V(vA.id()).id().next());

        final PartitionStrategy strategy = (PartitionStrategy) ((StrategyGraph) g).getStrategy();
        strategy.clearReadPartitions();

        try {
            g.V(vA.id());
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.elementNotFound(Vertex.class, vA.id());
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldThrowExceptionOneInDifferentPartition() {
        final Vertex vA = g.addVertex("any", "a");
        final Edge e = vA.addEdge("knows", vA);
        assertEquals(e.id(), g.E(e.id()).id().next());

        final PartitionStrategy strategy = (PartitionStrategy) ((StrategyGraph) g).getStrategy();
        strategy.clearReadPartitions();

        try {
            g.E(e.id());
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.elementNotFound(Edge.class, e.id());
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldWriteToMultiplePartitions() {
        final Vertex vA = g.addVertex("any", "a");
        final Vertex vAA = g.addVertex("any", "aa");
        final Edge eAtoAA = vA.addEdge("a->a", vAA);

        final PartitionStrategy strategy = (PartitionStrategy) ((StrategyGraph) g).getStrategy();
        strategy.setWritePartition("B");
        final Vertex vB = g.addVertex("any", "b");
        vA.addEdge("a->b", vB);

        strategy.setWritePartition("C");
        final Vertex vC = g.addVertex("any", "c");
        final Edge eBtovC = vB.addEdge("b->c", vC);
        final Edge eAtovC = vA.addEdge("a->c", vC);

        strategy.clearReadPartitions();
        assertEquals(new Long(0), g.V().count().next());
        assertEquals(new Long(0), g.E().count().next());

        strategy.addReadPartition("A");
        assertEquals(new Long(2), g.V().count().next());
        assertEquals(new Long(1), g.E().count().next());
        assertEquals(new Long(1), g.V(vA.id()).outE().count().next());
        assertEquals(eAtoAA.id(), g.V(vA.id()).outE().next().id());
        assertEquals(new Long(1), g.V(vA.id()).out().count().next());
        assertEquals(vAA.id(), g.V(vA.id()).out().next().id());

        final Vertex vA1 = g.V(vA.id()).next();
        assertEquals(new Long(1), vA1.outE().count().next());
        assertEquals(eAtoAA.id(), vA1.outE().next().id());
        assertEquals(new Long(1), vA1.out().count().next());
        assertEquals(vAA.id(), vA1.out().next().id());

        strategy.addReadPartition("B");
        assertEquals(new Long(3), g.V().count().next());
        assertEquals(new Long(2), g.E().count().next());

        strategy.addReadPartition("C");
        assertEquals(new Long(4), g.V().count().next());
        assertEquals(new Long(4), g.E().count().next());

        strategy.removeReadPartition("A");
        strategy.removeReadPartition("B");

        assertEquals(new Long(1), g.V().count().next());
        // two edges are in the "C" partition, but one each of their incident vertices are not
        assertEquals(new Long(0), g.E().count().next());

        assertEquals(new Long(0), g.V(vC.id()).inE().count().next());
        assertEquals(new Long(0), g.V(vC.id()).in().count().next());

        strategy.addReadPartition("B");
        // only one edge in, due to excluded vertices; vA is not in {B,C}
        assertEquals(new Long(1), g.V(vC.id()).inE().count().next());
        assertEquals(new Long(1), g.V(vC.id()).in().count().next());
        assertEquals(vC.id(), g.E(eBtovC.id()).inV().id().next());
        assertEquals(vB.id(), g.E(eBtovC.id()).outV().id().next());

        try {
            g.E(eAtovC.id()).next();
            fail("Edge should not be in the graph because vA is not in partitions {B,C}");
        } catch (Exception ex) {
            assertTrue(ex instanceof NoSuchElementException);
        }
    }
}
