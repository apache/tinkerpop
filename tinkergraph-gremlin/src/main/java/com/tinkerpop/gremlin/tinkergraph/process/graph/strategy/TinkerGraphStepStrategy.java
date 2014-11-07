package com.tinkerpop.gremlin.tinkergraph.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect.TinkerGraphStep;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphStepStrategy extends AbstractTraversalStrategy {

    private static final TinkerGraphStepStrategy INSTANCE = new TinkerGraphStepStrategy();
    private final static Set<Class<? extends TraversalStrategy>> POSTS = Stream.of(TraverserSourceStrategy.class).collect(Collectors.toSet());

    private TinkerGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER))
            return;

        final TinkerGraphStep<?> tinkerGraphStep = (TinkerGraphStep) TraversalHelper.getStart(traversal);
        Step<?, ?> currentStep = tinkerGraphStep.getNextStep();
        while (true) {
            if (currentStep == EmptyStep.instance() || TraversalHelper.isLabeled(currentStep)) break;
            if (currentStep instanceof HasStep) {
                tinkerGraphStep.hasContainers.addAll(((HasStep) currentStep).getHasContainers());
                TraversalHelper.removeStep(currentStep, traversal);
            } else if (currentStep instanceof IntervalStep) {
                tinkerGraphStep.hasContainers.addAll(((IntervalStep) currentStep).getHasContainers());
                TraversalHelper.removeStep(currentStep, traversal);
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return POSTS;
    }

    public static TinkerGraphStepStrategy instance() {
        return INSTANCE;
    }
}
