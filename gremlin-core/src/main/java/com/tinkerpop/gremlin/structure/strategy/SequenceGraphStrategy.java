package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SequenceGraphStrategy implements GraphStrategy {
    private final List<GraphStrategy> graphStrategySequence;

    public SequenceGraphStrategy(final GraphStrategy... strategies) {
        this.graphStrategySequence = new ArrayList<>(Arrays.asList(strategies));
    }

    @Override
    public GraphTraversal applyStrategyToTraversal(final GraphTraversal traversal) {
        final UnaryOperator<GraphTraversal> composedStrategy = graphStrategySequence.stream().map(SequenceGraphStrategy::convertToUnaryOperator)
                .reduce(null, (acc, next) -> acc == null ? next : toUnaryOp(acc.compose(next)));
        return composedStrategy.apply(traversal);
    }

    public static UnaryOperator<GraphTraversal> convertToUnaryOperator(final GraphStrategy s) {
        return s::applyStrategyToTraversal;
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getAddVertexStrategy(ctx));
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getAddEdgeStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getRemoveEdgeStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getRemoveVertexStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final Strategy.Context<StrategyWrappedProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getRemovePropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String, VertexProperty<V>>> getVertexGetPropertyStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexGetPropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String, Property<V>>> getEdgeGetPropertyStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeGetPropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, VertexProperty<V>>> getVertexPropertyStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, Property<V>>> getEdgePropertyStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgePropertyStrategy(ctx));
    }

    @Override
    public <V, U> UnaryOperator<BiFunction<String, V, Property<V>>> getVertexPropertyPropertyStrategy(final Strategy.Context<StrategyWrappedVertexProperty<U>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyPropertyStrategy(ctx));
    }

    @Override
    public UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphvStrategy(ctx));
    }

    @Override
    public UnaryOperator<Function<Object, Edge>> getGrapheStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGrapheStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Object>> getVertexIdStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIdStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Graph>> getVertexGraphStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexGraphStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Object>> getVertexPropertyIdStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIdStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Graph>> getVertexPropertyGraphStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyGraphStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Object>> getEdgeIdStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIdStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Graph>> getEdgeGraphStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeGraphStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<String>> getVertexLabelStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexLabelStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<String>> getEdgeLabelStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeLabelStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<String>> getVertexPropertyLabelStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyLabelStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsPropertiesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsPropertiesStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<Property<V>>>> getEdgeIteratorsPropertiesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsPropertiesStrategy(ctx));
    }

    @Override
    public <V, U> UnaryOperator<Function<String[], Iterator<Property<V>>>> getVertexPropertyIteratorsPropertiesStrategy(final Strategy.Context<StrategyWrappedVertexProperty<U>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIteratorsPropertiesStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsHiddensStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsHiddensStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<? extends Property<V>>>> getEdgeIteratorsHiddensStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsHiddensStrategy(ctx));
    }

    @Override
    public <V, U> UnaryOperator<Function<String[], Iterator<? extends Property<V>>>> getVertexPropertyIteratorsHiddensStrategy(Strategy.Context<StrategyWrappedVertexProperty<U>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIteratorsHiddensStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexKeysStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getEdgeKeysStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeKeysStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Set<String>>> getVertexPropertyKeysStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyKeysStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVertexHiddenKeysStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexHiddenKeysStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getEdgeHiddenKeysStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeHiddenKeysStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Set<String>>> getVertexPropertyHiddenKeysStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyHiddenKeysStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsValuesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsValuesStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getEdgeIteratorsValuesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsValuesStrategy(ctx));
    }

    @Override
    public <V, U> UnaryOperator<Function<String[], Iterator<V>>> getVertexPropertyIteratorsValuesStrategy(final Strategy.Context<StrategyWrappedVertexProperty<U>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIteratorsValuesStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsHiddenValuesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsHiddenValuesStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getEdgeIteratorsHiddenValuesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsHiddenValuesStrategy(ctx));
    }

    @Override
    public <V, U> UnaryOperator<Function<String[], Iterator<V>>> getVertexPropertyIteratorsHiddenValuesStrategy(final Strategy.Context<StrategyWrappedVertexProperty<U>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIteratorsHiddenValuesStrategy(ctx));
    }

    @Override
    public UnaryOperator<Function<Direction, Iterator<Vertex>>> getEdgeIteratorsVerticesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsVerticesStrategy(ctx));
    }

    @Override
    public UnaryOperator<BiFunction<Direction, String[], Iterator<Vertex>>> getVertexIteratorsVerticesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsVerticesStrategy(ctx));
    }

    @Override
    public UnaryOperator<BiFunction<Direction, String[], Iterator<Edge>>> getVertexIteratorsEdgesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsEdgesStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String, V>> getVertexValueStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexValueStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String, V>> getEdgeValueStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeValueStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVariableKeysStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableKeysStrategy(ctx));
    }

    @Override
    public <R> UnaryOperator<Function<String, Optional<R>>> getVariableGetStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableGetStrategy(ctx));
    }

    @Override
    public UnaryOperator<Consumer<String>> getVariableRemoveStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableRemoveStrategy(ctx));
    }

    @Override
    public UnaryOperator<BiConsumer<String, Object>> getVariableSetStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableSetStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Map<String, Object>>> getVariableAsMapStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableAsMapStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getGraphCloseStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphCloseStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemoveVertexPropertyStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getRemoveVertexPropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Vertex>> getVertexPropertyGetElementStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyGetElementStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<GraphTraversal<Vertex, Vertex>>> getGraphVStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphVStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<GraphTraversal<Edge, Edge>>> getGraphEStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphEStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<GraphTraversal>> getGraphOfStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphOfStrategy(ctx));
    }

    @Override
    public String toString() {
        return String.join("->", graphStrategySequence.stream().map(Object::toString)
                .map(String::toLowerCase).collect(Collectors.<String>toList()));
    }

    /**
     * Compute a new strategy function from the sequence of supplied {@link GraphStrategy} objects.
     *
     * @param f a {@link java.util.function.Function} that extracts a particular strategy implementation from a {@link GraphStrategy}
     * @return a newly constructed {@link java.util.function.UnaryOperator} that applies each extracted strategy implementation in
     * the order supplied
     */
    private UnaryOperator composeStrategyUnaryOperator(final Function<GraphStrategy, UnaryOperator> f) {
        return this.graphStrategySequence.stream().map(f).reduce(null,
                (acc, next) -> acc == null ? next : toUnaryOp(acc.compose(next)));
    }

    /**
     * Converts a {@link java.util.function.Function} to a {@link java.util.function.UnaryOperator} since the call to
     * {@link java.util.function.UnaryOperator#andThen(java.util.function.Function)} doesn't return {@link java.util.function.UnaryOperator} and can't
     * be casted to one.
     *
     * @param f a {@link java.util.function.Function} that has the same argument and return type
     * @return a {@link java.util.function.UnaryOperator} of the supplied {@code f}
     */
    private static <T> UnaryOperator<T> toUnaryOp(final Function<T, T> f) {
        return new UnaryOperator<T>() {
            @Override
            public T apply(T t) {
                return f.apply(t);
            }
        };
    }
}