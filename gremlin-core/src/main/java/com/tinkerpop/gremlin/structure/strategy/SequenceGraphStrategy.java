package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.STriFunction;

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
    public UnaryOperator<STriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getAddEdgeStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveElementStrategy(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getRemoveElementStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(Strategy.Context<StrategyWrappedProperty<V>> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getRemovePropertyStrategy(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String, ? extends Property<V>>> getElementGetProperty(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementGetProperty(ctx));
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, ? extends Property<V>>> getElementProperty(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementProperty(ctx));
    }

    @Override
    public UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(Strategy.Context<StrategyWrappedGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphvStrategy(ctx));
    }

    @Override
    public UnaryOperator<Function<Object, Edge>> getGrapheStrategy(Strategy.Context<StrategyWrappedGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGrapheStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Object>> getElementId(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementId(ctx));
    }

    @Override
    public UnaryOperator<Supplier<String>> getElementLabel(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementLabel(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[],Iterator<? extends Property<V>>>> getElementPropertiesGetter(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementId(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[],Iterator<? extends Property<V>>>> getElementHiddens(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementId(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getElementKeys(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementKeys(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getElementHiddenKeys(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementHiddenKeys(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[],Iterator<V>>> getElementValues(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementValues(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String[],Iterator<V>>> getElementHiddenValues(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementHiddenValues(ctx));
    }

    @Override
    public <V> UnaryOperator<Function<String, V>> getElementValue(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getElementValue(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVariableKeysStrategy(Strategy.Context<StrategyWrappedVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableKeysStrategy(ctx));
    }

    @Override
    public <R> UnaryOperator<Function<String, Optional<R>>> getVariableGetStrategy(Strategy.Context<StrategyWrappedVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableGetStrategy(ctx));
    }

    @Override
    public UnaryOperator<Consumer<String>> getVariableRemoveStrategy(Strategy.Context<StrategyWrappedVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableRemoveStrategy(ctx));
    }

    @Override
    public UnaryOperator<BiConsumer<String, Object>> getVariableSetStrategy(Strategy.Context<StrategyWrappedVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableSetStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Map<String, Object>>> getVariableAsMapStrategy(Strategy.Context<StrategyWrappedVariables> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getVariableAsMapStrategy(ctx));
    }

    @Override
    public UnaryOperator<Supplier<Void>> getGraphClose(Strategy.Context<StrategyWrappedGraph> ctx) {
        return this.composeStrategyUnaryOperator(s -> s.getGraphClose(ctx));
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