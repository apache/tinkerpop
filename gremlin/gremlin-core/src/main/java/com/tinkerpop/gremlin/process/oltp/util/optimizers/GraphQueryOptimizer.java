package com.tinkerpop.gremlin.process.oltp.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.filter.HasStep;
import com.tinkerpop.gremlin.process.oltp.filter.IntervalStep;
import com.tinkerpop.gremlin.process.oltp.map.GraphQueryStep;
import com.tinkerpop.gremlin.process.oltp.map.IdentityStep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryOptimizer implements Optimizer.StepOptimizer {

    private static final List<Class> PIPES_TO_FOLD = new ArrayList<Class>(
            Arrays.asList(
                    IdentityStep.class,
                    HasStep.class,
                    IntervalStep.class));

    public boolean optimize(final Traversal traversal, final Step step) {
        if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(step.getClass())).findFirst().isPresent())
            return true;

        GraphQueryStep graphQueryPipe = null;
        for (int i = traversal.getSteps().size() - 1; i >= 0; i--) {
            final Step tempStep = (Step) traversal.getSteps().get(i);
            if (tempStep instanceof GraphQueryStep) {
                graphQueryPipe = (GraphQueryStep) tempStep;
                break;
            } else if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(tempStep.getClass())).findFirst().isPresent())
                break;
        }

        if (null != graphQueryPipe) {
            if (step instanceof HasStep) {
                final HasStep hasPipe = (HasStep) step;
                graphQueryPipe.queryBuilder.has(hasPipe.hasContainer.key, hasPipe.hasContainer.predicate, hasPipe.hasContainer.value);
            } else if (step instanceof IntervalStep) {
                final IntervalStep intervalPipe = (IntervalStep) step;
                graphQueryPipe.queryBuilder.has(intervalPipe.startContainer.key, intervalPipe.startContainer.predicate, intervalPipe.startContainer.value);
                graphQueryPipe.queryBuilder.has(intervalPipe.endContainer.key, intervalPipe.endContainer.predicate, intervalPipe.endContainer.value);
            }
            graphQueryPipe.generateHolderIterator(false);
            return false;
        }
        return true;
    }
}
