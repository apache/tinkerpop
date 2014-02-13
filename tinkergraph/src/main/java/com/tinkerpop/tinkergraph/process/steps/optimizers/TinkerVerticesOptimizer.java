package com.tinkerpop.tinkergraph.process.steps.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.filter.HasStep;
import com.tinkerpop.gremlin.process.steps.filter.IntervalStep;
import com.tinkerpop.gremlin.process.steps.map.IdentityStep;
import com.tinkerpop.tinkergraph.process.steps.map.TinkerVerticesStep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVerticesOptimizer implements Optimizer.StepOptimizer {

    private static final List<Class> PIPES_TO_FOLD = new ArrayList<Class>(
            Arrays.asList(
                    IdentityStep.class,
                    HasStep.class,
                    IntervalStep.class));

    public boolean optimize(final Traversal traversal, final Step step) {
        if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(step.getClass())).findFirst().isPresent())
            return true;

        TinkerVerticesStep tinkerVerticesStep = null;
        for (int i = traversal.getSteps().size() - 1; i >= 0; i--) {
            final Step tempStep = (Step) traversal.getSteps().get(i);
            if (tempStep instanceof TinkerVerticesStep) {
                tinkerVerticesStep = (TinkerVerticesStep) tempStep;
                break;
            } else if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(tempStep.getClass())).findFirst().isPresent())
                break;
        }

        if (null != tinkerVerticesStep) {
            if (step instanceof HasStep) {
                final HasStep hasPipe = (HasStep) step;
                tinkerVerticesStep.hasContainers.add(hasPipe.hasContainer);
            } else if (step instanceof IntervalStep) {
                final IntervalStep intervalPipe = (IntervalStep) step;
                tinkerVerticesStep.hasContainers.add(intervalPipe.startContainer);
                tinkerVerticesStep.hasContainers.add(intervalPipe.endContainer);
            }
            tinkerVerticesStep.generateHolderIterator(false);
            return false;
        }
        return true;
    }
}
