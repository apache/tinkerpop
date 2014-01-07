package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SequenceGraphStrategy implements GraphStrategy {
    private final List<GraphStrategy> graphStrategySequence;

    public SequenceGraphStrategy(final GraphStrategy... strategies) {
        this.graphStrategySequence = new ArrayList<>(Arrays.asList(strategies));
    }

    @Override
    public UnaryOperator<Object[]> getPreAddVertex() {
        return this.computeStrategyUnaryOperator(s -> s.getPreAddVertex());
    }

    @Override
    public UnaryOperator<Vertex> getPostAddVertex() {
        return this.computeStrategyUnaryOperator(s -> s.getPostAddVertex());
    }

    @Override
    public UnaryOperator<Triplet<String, Vertex, Object[]>> getPreAddEdge() {
        return this.computeStrategyUnaryOperator(s -> s.getPreAddEdge());
    }

    @Override
    public UnaryOperator<Edge> getPostAddEdge() {
        return this.computeStrategyUnaryOperator(s -> s.getPostAddEdge());
    }

    @Override
    public Consumer<Vertex> getPreRemoveVertex() {
        return this.computeStrategyConsumer(s -> s.getPreRemoveVertex());
    }

    @Override
    public Consumer<Vertex> getPostRemoveVertex() {
        return this.computeStrategyConsumer(s -> s.getPostRemoveVertex());
    }

    /**
     * Compute a new strategy function from the sequence of supplied {@link GraphStrategy} objects.
     *
     * @param f a {@link Function} that extracts a particular strategy implementation from a {@link GraphStrategy}
     * @return a newly constructed {@link UnaryOperator} that applies each extracted strategy implementation in
     *         the order supplied
     */
    private UnaryOperator computeStrategyUnaryOperator(final Function<GraphStrategy, UnaryOperator> f) {
        return this.graphStrategySequence.stream().map(f).reduce(null,
                (acc, next) -> acc == null ? next : toUnaryOp(acc.andThen(next)));
    }

    private Consumer computeStrategyConsumer(final Function<GraphStrategy, Consumer> f) {
        return this.graphStrategySequence.stream().map(f).reduce(null,
                (acc, next) -> acc == null ? next : acc.andThen(next));
    }

    /**
     * Converts a {@link Function} to a {@link UnaryOperator} since the call to
     * {@link UnaryOperator#andThen(java.util.function.Function)} doesn't return {@link UnaryOperator} and can't
     * be casted to one.
     *
     * @param f a {@link Function} that has the same argument and return type
     * @return a {@link UnaryOperator} of the supplied {@code f}
     */
    private static <T> UnaryOperator<T> toUnaryOp(final Function<T,T> f) {
        return new UnaryOperator<T>() {
            @Override
            public T apply(T t) {
                return f.apply(t);
            }
        };
    }
}