package com.tinkerpop.gremlin.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.traversal.__;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.AndStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.ConjunctionStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.OrStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.traversal.step.EmptyStep;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ConjunctionStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final ConjunctionStrategy INSTANCE = new ConjunctionStrategy();

    private ConjunctionStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine traversalEngine) {
        processConjunctionMarker(AndStep.AndMarker.class, traversal);
        processConjunctionMarker(OrStep.OrMarker.class, traversal);
    }

    private static final boolean legalCurrentStep(final Step<?, ?> step) {
        return !(step instanceof EmptyStep || step instanceof OrStep.OrMarker || step instanceof AndStep.AndMarker || step instanceof StartStep);
    }

    private static final void processConjunctionMarker(final Class<? extends ConjunctionStep.ConjunctionMarker> markerClass, final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(markerClass, traversal).forEach(markerStep -> {
            Step<?, ?> currentStep = markerStep.getNextStep();
            final Traversal.Admin<?, ?> rightTraversal = __.start().asAdmin();
            while (legalCurrentStep(currentStep)) {
                final Step<?, ?> nextStep = currentStep.getNextStep();
                rightTraversal.addStep(currentStep);
                traversal.removeStep(currentStep);
                currentStep = nextStep;
            }

            currentStep = markerStep.getPreviousStep();
            final Traversal.Admin<?, ?> leftTraversal = __.start().asAdmin();
            while (legalCurrentStep(currentStep)) {
                final Step<?, ?> previousStep = currentStep.getPreviousStep();
                leftTraversal.addStep(0, currentStep);
                traversal.removeStep(currentStep);
                currentStep = previousStep;
            }
            TraversalHelper.replaceStep(markerStep,
                    markerClass.equals(AndStep.AndMarker.class) ?
                            new AndStep<Object>(traversal, (Traversal.Admin) leftTraversal, (Traversal.Admin) rightTraversal) :
                            new OrStep<Object>(traversal, (Traversal.Admin) leftTraversal, (Traversal.Admin) rightTraversal),
                    traversal);
        });
    }

    public static ConjunctionStrategy instance() {
        return INSTANCE;
    }
}
