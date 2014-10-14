package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.GraphStandardStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.LabeledEndStepStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalStrategyRegistry implements Traversal.Strategies {

    private static final List<TraversalStrategy> TRAVERSAL_STRATEGIES = new ArrayList<>();
    private static final TraversalStrategyRegistry INSTANCE = new TraversalStrategyRegistry();

    static {
        TRAVERSAL_STRATEGIES.add(TraverserSourceStrategy.instance());
        TRAVERSAL_STRATEGIES.add(LabeledEndStepStrategy.instance());
        TRAVERSAL_STRATEGIES.add(GraphStandardStrategy.instance());
    }

    private TraversalStrategyRegistry() {

    }

    public static TraversalStrategyRegistry instance() {
        return INSTANCE;
    }

    public static void populate(final Traversal.Strategies strategies) {
        for (final TraversalStrategy strategy : TRAVERSAL_STRATEGIES) {
            strategies.register(strategy);
        }
    }

    @Override
    public void register(final TraversalStrategy traversalStrategy) {
        if (!TRAVERSAL_STRATEGIES.contains(traversalStrategy))
            TRAVERSAL_STRATEGIES.add(traversalStrategy);

    }

    @Override
    public void unregister(final Class<? extends TraversalStrategy> optimizerClass) {
        TRAVERSAL_STRATEGIES.stream().filter(c -> optimizerClass.isAssignableFrom(c.getClass()))
                .collect(Collectors.toList())
                .forEach(TRAVERSAL_STRATEGIES::remove);
    }

    @Override
    public void apply() {
        throw new UnsupportedOperationException("The global registry is not tied to a traversal, only accessible by traversals");
    }

    @Override
    public void clear() {
        TRAVERSAL_STRATEGIES.clear();
    }

    @Override
    public boolean complete() {
        throw new UnsupportedOperationException("The global registry is not tied to a traversal, only accessible by traversals");
    }

    @Override
    public String toString() {
        return TRAVERSAL_STRATEGIES.toString();
    }

}
