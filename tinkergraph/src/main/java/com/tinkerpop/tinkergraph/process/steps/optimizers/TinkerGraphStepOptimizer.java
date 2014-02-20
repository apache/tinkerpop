package com.tinkerpop.tinkergraph.process.steps.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.filter.HasStep;
import com.tinkerpop.gremlin.process.steps.filter.IntervalStep;
import com.tinkerpop.gremlin.process.steps.map.IdentityStep;
import com.tinkerpop.gremlin.process.steps.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.tinkergraph.process.steps.map.TinkerGraphStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphStepOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Traversal traversal) {

        if (traversal.getSteps().get(0) instanceof TinkerGraphStep) {  // TODO: generalize for searching the whole traversal?
            final TinkerGraphStep tinkerGraphStep = (TinkerGraphStep) traversal.getSteps().get(0);
            Step currentStep = tinkerGraphStep.getNextStep();
            while (true) {
                if (currentStep == EmptyStep.instance() || TraversalHelper.isLabeled(currentStep)) break;

                if (currentStep instanceof HasStep) {
                    tinkerGraphStep.hasContainers.add(((HasStep) currentStep).hasContainer);
                    TraversalHelper.removeStep(currentStep, traversal);
                } else if (currentStep instanceof IntervalStep) {
                    tinkerGraphStep.hasContainers.add(((IntervalStep) currentStep).startContainer);
                    tinkerGraphStep.hasContainers.add(((IntervalStep) currentStep).endContainer);
                    TraversalHelper.removeStep(currentStep, traversal);
                } else if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    break;
                }
                currentStep = currentStep.getNextStep();
            }
            tinkerGraphStep.generateHolderIterator(false);
        }
    }
}
