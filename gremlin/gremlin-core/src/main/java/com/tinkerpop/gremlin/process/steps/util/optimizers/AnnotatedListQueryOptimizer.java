package com.tinkerpop.gremlin.process.steps.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.filter.HasStep;
import com.tinkerpop.gremlin.process.steps.filter.IntervalStep;
import com.tinkerpop.gremlin.process.steps.filter.RangeStep;
import com.tinkerpop.gremlin.process.steps.map.AnnotatedListQueryStep;
import com.tinkerpop.gremlin.process.steps.map.IdentityStep;
import com.tinkerpop.gremlin.process.steps.map.ValueStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedListQueryOptimizer implements Optimizer.StepOptimizer {

    private static final List<Class> PIPES_TO_FOLD = new ArrayList<Class>(
            Arrays.asList(
                    IdentityStep.class,
                    HasStep.class,
                    IntervalStep.class,
                    RangeStep.class,
                    ValueStep.class));

    public boolean optimize(final Traversal traversal, final Step step) {
        if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(step.getClass())).findFirst().isPresent())
            return true;
        else {
            if (TraversalHelper.isLabeled(step))
                return true;
        }

        AnnotatedListQueryStep annotatedListQueryPipe = null;
        for (int i = traversal.getSteps().size() - 1; i >= 0; i--) {
            final Step tempStep = (Step) traversal.getSteps().get(i);
            if (tempStep instanceof AnnotatedListQueryStep) {
                annotatedListQueryPipe = (AnnotatedListQueryStep) tempStep;
                break;
            } else if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(tempStep.getClass())).findFirst().isPresent())
                break;
        }

        if (null != annotatedListQueryPipe && !TraversalHelper.isLabeled(annotatedListQueryPipe)) {
            if (step instanceof ValueStep) {
                annotatedListQueryPipe.generateFunction(false);
            } else if (step instanceof HasStep) {
                final HasStep hasPipe = (HasStep) step;
                annotatedListQueryPipe.queryBuilder.has(hasPipe.hasContainer.key, hasPipe.hasContainer.predicate, hasPipe.hasContainer.value);
            } else if (step instanceof IntervalStep) {
                final IntervalStep intervalPipe = (IntervalStep) step;
                annotatedListQueryPipe.queryBuilder.has(intervalPipe.startContainer.key, intervalPipe.startContainer.predicate, intervalPipe.startContainer.value);
                annotatedListQueryPipe.queryBuilder.has(intervalPipe.endContainer.key, intervalPipe.endContainer.predicate, intervalPipe.endContainer.value);
            } else if (step instanceof RangeStep) {
                final RangeStep rangePipe = (RangeStep) step;
                annotatedListQueryPipe.low = rangePipe.low;
                annotatedListQueryPipe.high = rangePipe.high;
            }
            return false;
        }

        return true;
    }
}