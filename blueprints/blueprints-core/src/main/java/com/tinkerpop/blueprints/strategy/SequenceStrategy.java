package com.tinkerpop.blueprints.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SequenceStrategy implements Strategy {
    private final List<Strategy> strategySequence;

    public SequenceStrategy(final Strategy... strategies) {
        this.strategySequence = new ArrayList<>(Arrays.asList(strategies));
    }

    @Override
    public Function<Object[],Object[]> getPreAddVertex() {
        return this.strategySequence.stream().map(s -> s.getPreAddVertex()).reduce(null,
                (acc, next) -> acc == null ? next : acc.andThen(next));
    }
}