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
                final Traversal localTraversal = new DefaultGraphTraversal();
                Step previousStep = localTraversalStep.getPreviousStep();
                while (!(previousStep instanceof LocallyTraversable)) {
                    TraversalHelper.insertStep(previousStep.clone(), 0, localTraversal);
                    previousStep = previousStep.getPreviousStep();
                    if (previousStep instanceof EmptyStep) {
                        throw new IllegalStateException("The local() step must follow a vertex, edge, or vertex property generating step");
                    }
                }
                TraversalHelper.insertStep(new StartStep<>(localTraversal, null), 0, localTraversal);

                previousStep = localTraversalStep.getPreviousStep();
                while (!(previousStep instanceof LocallyTraversable)) {
                    TraversalHelper.removeStep(previousStep, traversal);
                    previousStep = previousStep.getPreviousStep();
                }
                ((LocallyTraversable) previousStep).setLocalTraversal(localTraversal);
                TraversalHelper.removeStep(localTraversalStep, traversal);

            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        });
    }

    public static LocalTraversalStrategy instance() {
        return INSTANCE;
    }
}
