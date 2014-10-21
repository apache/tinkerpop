package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphTraversalStrategyRegistry implements Traversal.Strategies {

    private static final List<TraversalStrategy> TRAVERSAL_STRATEGIES = new ArrayList<>();
    private static final GraphTraversalStrategyRegistry INSTANCE = new GraphTraversalStrategyRegistry();

    static {
        TRAVERSAL_STRATEGIES.add(TraverserSourceStrategy.instance());
        TRAVERSAL_STRATEGIES.add(LabeledEndStepStrategy.instance());
        TRAVERSAL_STRATEGIES.add(GraphStandardStrategy.instance());
        TRAVERSAL_STRATEGIES.add(UntilStrategy.instance());
        TRAVERSAL_STRATEGIES.add(DedupOptimizerStrategy.instance());
        TRAVERSAL_STRATEGIES.add(IdentityReductionStrategy.instance());
        TRAVERSAL_STRATEGIES.add(SideEffectCapStrategy.instance());
        TRAVERSAL_STRATEGIES.add(MatchWhereStrategy.instance());
    }

    private GraphTraversalStrategyRegistry() {

    }

    public static GraphTraversalStrategyRegistry instance() {
        return INSTANCE;
    }

    public static void populate(final Traversal.Strategies strategies) {
        for (final TraversalStrategy strategy : TRAVERSAL_STRATEGIES) {
            strategies.register(strategy);
        }
    }

    @Override
    public List<TraversalStrategy> toList() {
        return new ArrayList<>(TRAVERSAL_STRATEGIES);
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
