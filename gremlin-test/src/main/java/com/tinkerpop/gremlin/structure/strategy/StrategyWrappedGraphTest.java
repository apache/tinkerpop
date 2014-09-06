package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedGraphTest extends AbstractGremlinTest {
    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotCallBaseFunctionThusNotRemovingTheVertex() throws Exception {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);

        // create an ad-hoc strategy that only marks a vertex as "deleted" and removes all edges and properties
        // but doesn't actually blow it away
        swg.strategy().setGraphStrategy(new GraphStrategy() {
            @Override
            public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
                return (t) -> () -> {
                    final Vertex v = ctx.getCurrent().getBaseVertex();
                    v.bothE().remove();
                    v.properties().forEachRemaining(Property::remove);
                    v.property("deleted", true);
                    return null;
                };
            }
        });

        final Vertex toRemove = g.addVertex("name", "pieter");
        toRemove.addEdge("likes", g.addVertex("feature", "Strategy"));

        assertEquals(1, toRemove.properties().count().next().intValue());
        assertEquals(new Long(1), toRemove.bothE().count().next());
        assertFalse(toRemove.property("deleted").isPresent());

        swg.v(toRemove.id()).remove();

        final Vertex removed = g.v(toRemove.id());
        assertNotNull(removed);
        assertEquals(1, removed.properties().count().next().intValue());
        assertEquals(new Long(0), removed.bothE().count().next());
        assertTrue(removed.property("deleted").isPresent());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotCallBaseFunctionThusNotRemovingTheEdge() throws Exception {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);

        // create an ad-hoc strategy that only marks a vertex as "deleted" and removes all edges and properties
        // but doesn't actually blow it away
        swg.strategy().setGraphStrategy(new GraphStrategy() {
            @Override
            public UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
                return (t) -> () -> {
                    final Edge e = ctx.getCurrent().getBaseEdge();
                    e.properties().forEachRemaining(Property::remove);
                    e.property("deleted", true);
                    return null;
                };
            }
        });

        final Vertex v = g.addVertex("name", "pieter");
        final Edge e = v.addEdge("likes", g.addVertex("feature", "Strategy"), "this", "something");

        assertEquals(1, e.properties().count().next().intValue());
        assertFalse(e.property("deleted").isPresent());

        swg.e(e.id()).remove();

        final Edge removed = g.e(e.id());
        assertNotNull(removed);
        assertEquals(1, removed.properties().count().next().intValue());
        assertTrue(removed.property("deleted").isPresent());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldWrapProperties() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        final Vertex v = swg.addVertex();
        final Edge e = v.addEdge("to", v, "all", "a");

        assertTrue(e.property("all") instanceof StrategyWrappedProperty);
        assertTrue(StreamFactory.stream(e.properties()).allMatch(p -> p  instanceof StrategyWrappedProperty));

        assertTrue(g.E().properties("any").next() instanceof StrategyWrappedProperty);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldWrapMetaProperties() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        final Vertex v = swg.addVertex("any", "a");

        assertTrue(v.property("any") instanceof StrategyWrappedMetaProperty);
        assertTrue(StreamFactory.stream(v.properties()).allMatch(p -> p  instanceof StrategyWrappedMetaProperty));

        assertTrue(g.V().properties("any").next() instanceof StrategyWrappedMetaProperty);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldWrapETraversalEdges() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertEquals(6l, swg.E().count().next().longValue());
        swg.E().sideEffect(e -> assertTrue(e.get() instanceof StrategyWrappedEdge)).iterate();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldWrapVTraversalVertices() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertEquals(6l, swg.V().count().next().longValue());
        swg.V().sideEffect(e -> assertTrue(e.get() instanceof StrategyWrappedVertex)).iterate();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldWrapv() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertTrue(swg.v(convertToVertexId("marko")) instanceof StrategyWrappedVertex);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldWrape() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertTrue(swg.e(convertToEdgeId("josh", "created", "lop")) instanceof StrategyWrappedEdge);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldWrapvTraversalVertices() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertEquals(3l, swg.v(convertToVertexId("marko")).out().count().next().longValue());
        swg.v(convertToVertexId("marko")).out().sideEffect(e -> assertTrue(e.get() instanceof StrategyWrappedVertex)).iterate();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldWrapvTraversalEdges() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertEquals(3l, swg.v(convertToVertexId("marko")).outE().count().next().longValue());
        swg.v(convertToVertexId("marko")).outE().sideEffect(e -> assertTrue(e.get() instanceof StrategyWrappedEdge)).iterate();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldWrapvEdges() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertTrue(swg.v(convertToVertexId("marko")).iterators().edges(Direction.BOTH, 1).hasNext());
        assertTrue(swg.v(convertToVertexId("marko")).iterators().edges(Direction.BOTH, 1).next() instanceof StrategyWrappedEdge);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldWrapvAdjacentVertices() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertTrue(swg.v(convertToVertexId("marko")).iterators().vertices(Direction.BOTH, 1).hasNext());
        assertTrue(swg.v(convertToVertexId("marko")).iterators().vertices(Direction.BOTH, 1).next() instanceof StrategyWrappedVertex);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldWrapeDirectionVertices() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        final Object id = convertToEdgeId("josh", "created", "lop");
        assertTrue(swg.e(id).iterators().vertices(Direction.IN).hasNext());
        assertTrue(swg.e(id).iterators().vertices(Direction.IN).next() instanceof StrategyWrappedVertex);
        assertTrue(swg.e(id).iterators().vertices(Direction.OUT).hasNext());
        assertTrue(swg.e(id).iterators().vertices(Direction.OUT).next() instanceof StrategyWrappedVertex);
    }

    @Test
    public void shouldAdHocTheCloseStrategy() throws Exception {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);

        final AtomicInteger counter = new AtomicInteger(0);
        swg.strategy().setGraphStrategy(new GraphStrategy() {
            @Override
            public UnaryOperator<Supplier<Void>> getGraphClose(final Strategy.Context<StrategyWrappedGraph> ctx) {
                return (t) -> () -> {
                    counter.incrementAndGet();
                    return null;
                };
            }
        });

        // allows multiple calls to close() - the test will clean up with a call to the base graph.close()
        swg.close();
        swg.close();
        swg.close();

        assertEquals(3, counter.get());
    }
}
