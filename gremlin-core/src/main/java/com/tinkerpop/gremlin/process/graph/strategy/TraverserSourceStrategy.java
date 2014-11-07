package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserSourceStrategy extends AbstractTraversalStrategy {

    private static final TraverserSourceStrategy INSTANCE = new TraverserSourceStrategy();
    private static final Set<Class<? extends TraversalStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(ChooseLinearStrategy.class);
        PRIORS.add(ComparingRemovalStrategy.class);
        PRIORS.add(DedupOptimizerStrategy.class);
        PRIORS.add(EngineDependentStrategy.class);
        PRIORS.add(IdentityRemovalStrategy.class);
        PRIORS.add(LabeledEndStepStrategy.class);
        PRIORS.add(LocalRangeStrategy.class);
        PRIORS.add(MatchWhereStrategy.class);
        PRIORS.add(ReducingStrategy.class);
        PRIORS.add(SideEffectCapStrategy.class);
        PRIORS.add(UnionLinearStrategy.class);
        PRIORS.add(UnrollJumpStrategy.class);
        PRIORS.add(UntilStrategy.class);
    }


    private TraverserSourceStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER))
            return;

        final TraverserGenerator traverserGenerator = TraversalStrategies.GlobalCache.getStrategies(traversal.getClass()).getTraverserGenerator(traversal, engine);
        traversal.getSteps()
                .stream()
                .filter(step -> step instanceof TraverserSource)
                .forEach(step -> ((TraverserSource) step).generateTraversers(traverserGenerator));
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return PRIORS;
    }

    public static TraverserSourceStrategy instance() {
        return INSTANCE;
    }
}
