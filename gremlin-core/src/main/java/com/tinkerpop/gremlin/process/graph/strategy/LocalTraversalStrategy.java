package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.LocallyTraversable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.step.util.LocalTraversalStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalTraversalStrategy extends AbstractTraversalStrategy {

    private static final LocalTraversalStrategy INSTANCE = new LocalTraversalStrategy();

    private LocalTraversalStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        TraversalHelper.getStepsOfClass(LocalTraversalStep.class, traversal).forEach(localTraversalStep -> {
            try {

                TraversalHelper.removeStep(localTraversalStep, traversal);
                Step previousStep = localTraversalStep.getPreviousStep();
                final List<Step> stepsToAdd = new ArrayList<>();
                while (!(previousStep instanceof LocallyTraversable)) {
                    TraversalHelper.removeStep(previousStep,traversal);
                    stepsToAdd.add(previousStep);
                    previousStep = previousStep.getPreviousStep();
                    if (previousStep instanceof EmptyStep) {
                        throw new IllegalStateException("The local() step must follow a vertex, edge, or vertex property generating step");
                    }
                }

                final Traversal localTraversal = new DefaultGraphTraversal();
                for(final Step step : stepsToAdd) {
                    TraversalHelper.insertStep(step, 0, localTraversal);
                }
                TraversalHelper.insertStep(new StartStep<>(localTraversal, null), 0, localTraversal);
                ((LocallyTraversable) previousStep).setLocalTraversal(localTraversal);


            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        });
    }

    public static LocalTraversalStrategy instance() {
        return INSTANCE;
    }
}
