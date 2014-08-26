package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_STRING_VALUES;
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
            public UnaryOperator<Supplier<Void>> getRemoveElementStrategy(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
                if (ctx.getCurrent() instanceof StrategyWrappedVertex) {
                    return (t) -> () -> {
                        final Vertex v = ((StrategyWrappedVertex) ctx.getCurrent()).getBaseVertex();
                        v.bothE().remove();
                        v.properties().values().forEach(Property::remove);
                        v.property("deleted", true);
                        return null;
                    };
                } else {
                    return UnaryOperator.identity();
                }
            }
        });

        final Vertex toRemove = g.addVertex("name", "pieter");
        toRemove.addEdge("likes", g.addVertex("feature", "Strategy"));

        assertEquals(1, toRemove.properties().size());
        assertEquals(new Long(1), toRemove.bothE().count().next());
        assertFalse(toRemove.property("deleted").isPresent());

        swg.v(toRemove.id()).remove();

        final Vertex removed = g.v(toRemove.id());
        assertNotNull(removed);
        assertEquals(1, removed.properties().size());
        assertEquals(new Long(0), removed.bothE().count().next());
        assertTrue(toRemove.property("deleted").isPresent());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC_DOUBLE)
    public void shouldWrapETraversalEdges() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertEquals(6l, swg.E().count().next().longValue());
        swg.E().sideEffect(e -> assertTrue(e.get() instanceof StrategyWrappedEdge)).iterate();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC_DOUBLE)
    public void shouldWrapVTraversalVertices() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertEquals(6l, swg.V().count().next().longValue());
        swg.V().sideEffect(e -> assertTrue(e.get() instanceof StrategyWrappedVertex)).iterate();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC_DOUBLE)
    public void shouldWrapv() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertTrue(swg.v(convertToVertexId("marko")) instanceof StrategyWrappedVertex);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC_DOUBLE)
    public void shouldWrape() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertTrue(swg.e(convertToEdgeId("josh", "created", "lop")) instanceof StrategyWrappedEdge);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC_DOUBLE)
    public void shouldWrapvTraversalVertices() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertEquals(3l, swg.v(convertToVertexId("marko")).out().count().next().longValue());
        swg.v(convertToVertexId("marko")).out().sideEffect(e -> assertTrue(e.get() instanceof StrategyWrappedVertex)).iterate();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC_DOUBLE)
    public void shouldWrapvTraversalEdges() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertEquals(3l, swg.v(convertToVertexId("marko")).outE().count().next().longValue());
        swg.v(convertToVertexId("marko")).outE().sideEffect(e -> assertTrue(e.get() instanceof StrategyWrappedEdge)).iterate();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC_DOUBLE)
    public void shouldWrapvEdges() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertTrue(swg.v(convertToVertexId("marko")).edges(Direction.BOTH, 1).hasNext());
        assertTrue(swg.v(convertToVertexId("marko")).edges(Direction.BOTH, 1).next() instanceof StrategyWrappedEdge);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC_DOUBLE)
    public void shouldWrapvAdjacentVertices() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        assertTrue(swg.v(convertToVertexId("marko")).vertices(Direction.BOTH, 1).hasNext());
        assertTrue(swg.v(convertToVertexId("marko")).vertices(Direction.BOTH, 1).next() instanceof StrategyWrappedVertex);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC_DOUBLE)
    public void shouldWrapeDirectionVertices() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        final Object id = convertToEdgeId("josh", "created", "lop");
        assertTrue(swg.e(id).vertices(Direction.IN).hasNext());
        assertTrue(swg.e(id).vertices(Direction.IN).next() instanceof StrategyWrappedVertex);
        assertTrue(swg.e(id).vertices(Direction.OUT).hasNext());
        assertTrue(swg.e(id).vertices(Direction.OUT).next() instanceof StrategyWrappedVertex);
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
