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
import java.util.Collections;
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
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SequenceStrategy implements GraphStrategy {
    private final List<GraphStrategy> graphStrategySequence;

    private SequenceStrategy(final List<GraphStrategy> strategies) {
        this.graphStrategySequence = strategies;
    }

    public List<GraphStrategy> getGraphStrategySequence() {
        return Collections.unmodifiableList(graphStrategySequence);
    }

    @Override
    public UnaryOperator<Function<Object[], Iterator<Vertex>>> getGraphIteratorsVertexIteratorStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphIteratorsVertexIteratorStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Function<Object[], Iterator<Edge>>> getGraphIteratorsEdgeIteratorStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphIteratorsEdgeIteratorStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getAddVertexStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getAddEdgeStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getRemoveEdgeStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getRemoveVertexStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final StrategyContext<StrategyProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getRemovePropertyStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Function<String, VertexProperty<V>>> getVertexGetPropertyStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexGetPropertyStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Function<String, Property<V>>> getEdgeGetPropertyStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeGetPropertyStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, VertexProperty<V>>> getVertexPropertyStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, Property<V>>> getEdgePropertyStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgePropertyStrategy(ctx, this));
    }

    @Override
    public <V, U> UnaryOperator<BiFunction<String, V, Property<V>>> getVertexPropertyPropertyStrategy(final StrategyContext<StrategyVertexProperty<U>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyPropertyStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Object>> getVertexIdStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIdStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Graph>> getVertexGraphStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexGraphStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<Object>> getVertexPropertyIdStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIdStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<Graph>> getVertexPropertyGraphStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyGraphStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Object>> getEdgeIdStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIdStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Graph>> getEdgeGraphStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeGraphStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<String>> getVertexLabelStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexLabelStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<String>> getEdgeLabelStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeLabelStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<String>> getVertexPropertyLabelStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyLabelStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsPropertyIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsPropertyIteratorStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<Property<V>>>> getEdgeIteratorsPropertyIteratorStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsPropertyIteratorStrategy(ctx, this));
    }

    @Override
    public <V, U> UnaryOperator<Function<String[], Iterator<Property<V>>>> getVertexPropertyIteratorsPropertyIteratorStrategy(final StrategyContext<StrategyVertexProperty<U>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIteratorsPropertyIteratorStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexKeysStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getEdgeKeysStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeKeysStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<Set<String>>> getVertexPropertyKeysStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyKeysStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsValueIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsValueIteratorStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getEdgeIteratorsValueIteratorStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsValueIteratorStrategy(ctx, this));
    }

    @Override
    public <V, U> UnaryOperator<Function<String[], Iterator<V>>> getVertexPropertyIteratorsValueIteratorStrategy(final StrategyContext<StrategyVertexProperty<U>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIteratorsValueIteratorStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Function<Direction, Iterator<Vertex>>> getEdgeIteratorsVertexIteratorStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsVertexIteratorStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<BiFunction<Direction, String[], Iterator<Vertex>>> getVertexIteratorsVertexIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsVertexIteratorStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<BiFunction<Direction, String[], Iterator<Edge>>> getVertexIteratorsEdgeIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsEdgeIteratorStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Function<String, V>> getVertexValueStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexValueStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Function<String, V>> getEdgeValueStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeValueStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVariableKeysStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableKeysStrategy(ctx, this));
    }

    @Override
    public <R> UnaryOperator<Function<String, Optional<R>>> getVariableGetStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableGetStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Consumer<String>> getVariableRemoveStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableRemoveStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<BiConsumer<String, Object>> getVariableSetStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableSetStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Map<String, Object>>> getVariableAsMapStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableAsMapStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getGraphCloseStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphCloseStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemoveVertexPropertyStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getRemoveVertexPropertyStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<Vertex>> getVertexPropertyGetElementStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyGetElementStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Function<Object[], GraphTraversal<Vertex, Vertex>>> getGraphVStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphVStrategy(ctx, this));
    }

    @Override
    public UnaryOperator<Function<Object[], GraphTraversal<Edge, Edge>>> getGraphEStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphEStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<V>> getVertexPropertyValueStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyValueStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<V>> getPropertyValueStrategy(final StrategyContext<StrategyProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getPropertyValueStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<String>> getVertexPropertyKeyStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyKeyStrategy(ctx, this));
    }

    @Override
    public <V> UnaryOperator<Supplier<String>> getPropertyKeyStrategy(final StrategyContext<StrategyProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return this.composeStrategyUnaryOperator(s -> s.getPropertyKeyStrategy(ctx, this));
    }

    @Override
    public String toString() {
        return String.join("->", this.graphStrategySequence.stream().map(Object::toString)
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

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private List<GraphStrategy> strategies = new ArrayList<>();

        private Builder() {
            strategies.add(IdentityStrategy.instance());
        }

        /**
         * Provide the sequence of {@link GraphStrategy} implementations to execute.  If this value is not set,
         * then a {@code SequenceStrategy} is initialized with a single
         * {@link com.tinkerpop.gremlin.structure.strategy.IdentityStrategy} instance.
         */
        public Builder sequence(final GraphStrategy... strategies) {
            this.strategies = new ArrayList<>(Arrays.asList(strategies));
            return this;
        }

        public SequenceStrategy create() {
            return new SequenceStrategy(strategies);
        }
    }
}