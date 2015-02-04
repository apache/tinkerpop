package com.tinkerpop.gremlin.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.traversal.__;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.AndStep;
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

        TraversalHelper.getStepsOfClass(OrStep.OrMarker.class, traversal).forEach(orMarkerStep -> {
            Step<?, ?> currentStep = orMarkerStep.getNextStep();
            final Traversal.Admin<?, ?> rightTraversal = __.start().asAdmin();
            while (legalCurrentStep(currentStep)) {
                final Step<?, ?> nextStep = currentStep.getNextStep();
                rightTraversal.addStep(currentStep);
                traversal.removeStep(currentStep);
                currentStep = nextStep;
            }

            currentStep = orMarkerStep.getPreviousStep();
            final Traversal.Admin<?, ?> leftTraversal = __.start().asAdmin();
            while (legalCurrentStep(currentStep)) {
                final Step<?, ?> previousStep = currentStep.getPreviousStep();
                leftTraversal.addStep(0, currentStep);
                traversal.removeStep(currentStep);
                currentStep = previousStep;
            }
            final OrStep<?> orStep = new OrStep<Object>(traversal, (Traversal.Admin) leftTraversal, (Traversal.Admin) rightTraversal);
            TraversalHelper.replaceStep(orMarkerStep, orStep, traversal);

        });

        //////////////////////
        TraversalHelper.getStepsOfClass(AndStep.AndMarker.class, traversal).forEach(andMarkerStep -> {
            Step<?, ?> currentStep = andMarkerStep.getNextStep();
            final Traversal.Admin<?, ?> rightTraversal = __.start().asAdmin();
            while (legalCurrentStep(currentStep)) {
                final Step<?, ?> nextStep = currentStep.getNextStep();
                rightTraversal.addStep(currentStep);
                traversal.removeStep(currentStep);
                currentStep = nextStep;
            }

            currentStep = andMarkerStep.getPreviousStep();
            final Traversal.Admin<?, ?> leftTraversal = __.start().asAdmin();
            while (legalCurrentStep(currentStep)) {
                final Step<?, ?> previousStep = currentStep.getPreviousStep();
                leftTraversal.addStep(0, currentStep);
                traversal.removeStep(currentStep);
                currentStep = previousStep;
            }
            final AndStep<?> andStep = new AndStep<Object>(traversal, (Traversal.Admin) leftTraversal, (Traversal.Admin) rightTraversal);
            TraversalHelper.replaceStep(andMarkerStep, andStep, traversal);
        });
    }

    private static final boolean legalCurrentStep(final Step<?, ?> step) {
        return !(step instanceof EmptyStep || step instanceof OrStep.OrMarker || step instanceof AndStep.AndMarker || step instanceof StartStep);
    }

    public static ConjunctionStrategy instance() {
        return INSTANCE;
    }
}
