package com.tinkerpop.blueprints.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SequenceGraphStrategy implements GraphStrategy {
    private final List<GraphStrategy> graphStrategySequence;

    public SequenceGraphStrategy(final GraphStrategy... strategies) {
        this.graphStrategySequence = new ArrayList<>(Arrays.asList(strategies));
    }

    @Override
    public Function<Object[],Object[]> getPreAddVertex() {
        return this.graphStrategySequence.stream().map(s -> s.getPreAddVertex()).reduce(null,
                (acc, next) -> acc == null ? next : acc.andThen(next));
    }
}