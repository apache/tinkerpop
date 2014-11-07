package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.traversers.TraverserGeneratorFactory;
import com.tinkerpop.gremlin.process.traversers.util.DefaultTraverserGeneratorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphTraversalStrategyRegistry {

    private static final List<TraversalStrategy> TRAVERSAL_STRATEGIES = new ArrayList<>();
    private static final GraphTraversalStrategyRegistry INSTANCE = new GraphTraversalStrategyRegistry();
    private static TraverserGeneratorFactory TRAVERSER_GENERATOR_FACTORY = DefaultTraverserGeneratorFactory.instance();

    static {
        TRAVERSAL_STRATEGIES.add(TraverserSourceStrategy.instance());
        TRAVERSAL_STRATEGIES.add(LabeledEndStepStrategy.instance());
        TRAVERSAL_STRATEGIES.add(UntilStrategy.instance());
        TRAVERSAL_STRATEGIES.add(DedupOptimizerStrategy.instance());
        TRAVERSAL_STRATEGIES.add(IdentityRemovalStrategy.instance());
        TRAVERSAL_STRATEGIES.add(SideEffectCapStrategy.instance());
        TRAVERSAL_STRATEGIES.add(MatchWhereStrategy.instance());
        TRAVERSAL_STRATEGIES.add(ChooseLinearStrategy.instance());
        TRAVERSAL_STRATEGIES.add(UnionLinearStrategy.instance());
        TRAVERSAL_STRATEGIES.add(ComparingRemovalStrategy.instance());
        TRAVERSAL_STRATEGIES.add(EngineDependentStrategy.instance());
        TRAVERSAL_STRATEGIES.add(ReducingStrategy.instance());
        TRAVERSAL_STRATEGIES.add(LocalRangeStrategy.instance());
        //  TRAVERSAL_STRATEGIES.add(UnrollJumpStrategy.instance());
        TraversalStrategies.sortStrategies(TRAVERSAL_STRATEGIES);
    }

    private GraphTraversalStrategyRegistry() {

    }

    public static GraphTraversalStrategyRegistry instance() {
        return INSTANCE;
    }

    public List<TraversalStrategy> getTraversalStrategies() {
        return new ArrayList<>(TRAVERSAL_STRATEGIES);
    }

    public TraverserGeneratorFactory getTraverserGeneratorFactory() {
        return TRAVERSER_GENERATOR_FACTORY;
    }

    public void register(final TraversalStrategy traversalStrategy) {
        if (!TRAVERSAL_STRATEGIES.contains(traversalStrategy)) {
            TRAVERSAL_STRATEGIES.add(traversalStrategy);
            TraversalStrategies.sortStrategies(TRAVERSAL_STRATEGIES);
        }
    }

    public void register(final TraverserGeneratorFactory traverserGeneratorFactory) {
        TRAVERSER_GENERATOR_FACTORY = traverserGeneratorFactory;
    }

    public void unregister(final Class<? extends TraversalStrategy> traversalStrategyClass) {
        TRAVERSAL_STRATEGIES.stream().filter(c -> traversalStrategyClass.isAssignableFrom(c.getClass()))
                .collect(Collectors.toList())
                .forEach(TRAVERSAL_STRATEGIES::remove);
        TraversalStrategies.sortStrategies(TRAVERSAL_STRATEGIES);
    }

    @Override
    public String toString() {
        return TRAVERSAL_STRATEGIES.toString();
    }
}
