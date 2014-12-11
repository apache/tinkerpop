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
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SequenceStrategy implements GraphStrategy {
    private final List<GraphStrategy> graphStrategySequence;

    public SequenceStrategy(final GraphStrategy... strategies) {
        this.graphStrategySequence = new ArrayList<>(Arrays.asList(strategies));
    }

    public void add(final GraphStrategy strategy) {
        this.graphStrategySequence.add(strategy);
    }

    @Override
    public UnaryOperator<Function<Object[], Iterator<Vertex>>> getGraphIteratorsVertexIteratorStrategy(final Strategy.StrategyContext<StrategyGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphIteratorsVertexIteratorStrategy(ctx));
    }

    @Override
    public UnaryOperator<Function<Object[], Iterator<Edge>>> getGraphIteratorsEdgeIteratorStrategy(final Strategy.StrategyContext<StrategyGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphIteratorsEdgeIteratorStrategy(ctx));
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.StrategyContext<StrategyGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getAddVertexStrategy(ctx));
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getAddEdgeStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getRemoveEdgeStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getRemoveVertexStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final Strategy.StrategyContext<StrategyProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getRemovePropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String, VertexProperty<V>>> getVertexGetPropertyStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexGetPropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String, Property<V>>> getEdgeGetPropertyStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeGetPropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, VertexProperty<V>>> getVertexPropertyStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, Property<V>>> getEdgePropertyStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgePropertyStrategy(ctx));
    }

    @Override
    public <V, U> UnaryOperator<BiFunction<String, V, Property<V>>> getVertexPropertyPropertyStrategy(final Strategy.StrategyContext<StrategyVertexProperty<U>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyPropertyStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Object>> getVertexIdStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIdStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Graph>> getVertexGraphStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexGraphStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Object>> getVertexPropertyIdStrategy(final Strategy.StrategyContext<StrategyVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIdStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Graph>> getVertexPropertyGraphStrategy(final Strategy.StrategyContext<StrategyVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyGraphStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Object>> getEdgeIdStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIdStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Graph>> getEdgeGraphStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeGraphStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<String>> getVertexLabelStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexLabelStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<String>> getEdgeLabelStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeLabelStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<String>> getVertexPropertyLabelStrategy(final Strategy.StrategyContext<StrategyVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyLabelStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsPropertyIteratorStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsPropertyIteratorStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<Property<V>>>> getEdgeIteratorsPropertyIteratorStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsPropertyIteratorStrategy(ctx));
    }

    @Override
    public <V, U> UnaryOperator<Function<String[], Iterator<Property<V>>>> getVertexPropertyIteratorsPropertyIteratorStrategy(final Strategy.StrategyContext<StrategyVertexProperty<U>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIteratorsPropertyIteratorStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexKeysStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getEdgeKeysStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeKeysStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Set<String>>> getVertexPropertyKeysStrategy(final Strategy.StrategyContext<StrategyVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyKeysStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsValueIteratorStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsValueIteratorStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getEdgeIteratorsValueIteratorStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsValueIteratorStrategy(ctx));
    }

    @Override
    public <V, U> UnaryOperator<Function<String[], Iterator<V>>> getVertexPropertyIteratorsValueIteratorStrategy(final Strategy.StrategyContext<StrategyVertexProperty<U>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyIteratorsValueIteratorStrategy(ctx));
    }

    @Override
    public UnaryOperator<Function<Direction, Iterator<Vertex>>> getEdgeIteratorsVertexIteratorStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeIteratorsVertexIteratorStrategy(ctx));
    }

    @Override
    public UnaryOperator<BiFunction<Direction, String[], Iterator<Vertex>>> getVertexIteratorsVertexIteratorStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsVertexIteratorStrategy(ctx));
    }

    @Override
    public UnaryOperator<BiFunction<Direction, String[], Iterator<Edge>>> getVertexIteratorsEdgeIteratorStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexIteratorsEdgeIteratorStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String, V>> getVertexValueStrategy(final Strategy.StrategyContext<StrategyVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexValueStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String, V>> getEdgeValueStrategy(final Strategy.StrategyContext<StrategyEdge> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getEdgeValueStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVariableKeysStrategy(final Strategy.StrategyContext<StrategyVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableKeysStrategy(ctx));
    }

    @Override
    public <R> UnaryOperator<Function<String, Optional<R>>> getVariableGetStrategy(final Strategy.StrategyContext<StrategyVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableGetStrategy(ctx));
    }

    @Override
    public UnaryOperator<Consumer<String>> getVariableRemoveStrategy(final Strategy.StrategyContext<StrategyVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableRemoveStrategy(ctx));
    }

    @Override
    public UnaryOperator<BiConsumer<String, Object>> getVariableSetStrategy(final Strategy.StrategyContext<StrategyVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableSetStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Map<String, Object>>> getVariableAsMapStrategy(final Strategy.StrategyContext<StrategyVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableAsMapStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getGraphCloseStrategy(final Strategy.StrategyContext<StrategyGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphCloseStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemoveVertexPropertyStrategy(final Strategy.StrategyContext<StrategyVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getRemoveVertexPropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Vertex>> getVertexPropertyGetElementStrategy(final Strategy.StrategyContext<StrategyVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyGetElementStrategy(ctx));
    }

    @Override
    public UnaryOperator<Function<Object[], GraphTraversal<Vertex, Vertex>>> getGraphVStrategy(final Strategy.StrategyContext<StrategyGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphVStrategy(ctx));
    }

    @Override
    public UnaryOperator<Function<Object[], GraphTraversal<Edge, Edge>>> getGraphEStrategy(final Strategy.StrategyContext<StrategyGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphEStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<GraphTraversal>> getGraphOfStrategy(final Strategy.StrategyContext<StrategyGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphOfStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<V>> getVertexPropertyValueStrategy(final Strategy.StrategyContext<StrategyVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyValueStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<V>> getPropertyValueStrategy(final Strategy.StrategyContext<StrategyProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getPropertyValueStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<String>> getVertexPropertyKeyStrategy(final Strategy.StrategyContext<StrategyVertexProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVertexPropertyKeyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<String>> getPropertyKeyStrategy(final Strategy.StrategyContext<StrategyProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getPropertyKeyStrategy(ctx));
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
}