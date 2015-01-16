package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.step.branch.ChooseStep;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionStep;
import com.tinkerpop.gremlin.process.graph.step.branch.util.RouteStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RouteStrategy extends AbstractTraversalStrategy {

    private static final RouteStrategy INSTANCE = new RouteStrategy();

    private RouteStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD))
            return;

        TraversalHelper.getStepsOfClass(UnionStep.class, traversal).stream()
                .forEach(step -> {
                    for (final Traversal<?, ?> t : ((UnionStep<?, ?>) step).getTraversals()) {
                        final RouteStep routeStep = new RouteStep(t, step.getNextStep() instanceof EmptyStep ?
                                step.getTraversal().asAdmin().getTraversalHolder().asStep().getNextStep().getLabel() :
                                step.getNextStep().getLabel());   // TODO: walk indefinately?
                        t.asAdmin().addStep(routeStep);
                    }
                });

        TraversalHelper.getStepsOfClass(ChooseStep.class, traversal).stream()
                .forEach(step -> {
                    for (final Traversal<?, ?> t : ((ChooseStep<?, ?, ?>) step).getTraversals()) {
                        final RouteStep routeStep = new RouteStep(t, step.getNextStep() instanceof EmptyStep ?
                                step.getTraversal().asAdmin().getTraversalHolder().asStep().getNextStep().getLabel() :
                                step.getNextStep().getLabel());   // TODO: walk indefinately?
                        t.asAdmin().addStep(routeStep);
                    }
                });
    }

    public static RouteStrategy instance() {
        return INSTANCE;
    }
}