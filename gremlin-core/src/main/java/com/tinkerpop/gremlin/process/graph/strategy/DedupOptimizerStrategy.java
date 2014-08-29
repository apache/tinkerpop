package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.filter.DedupStep;
import com.tinkerpop.gremlin.process.graph.step.util.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.map.OrderStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DedupOptimizerStrategy implements TraversalStrategy.NoDependencies{

    private static final DedupOptimizerStrategy INSTANCE = new DedupOptimizerStrategy();

    private DedupOptimizerStrategy() {
    }

    private static final List<Class> BIJECTIVE_PIPES = new ArrayList<Class>(
            Arrays.asList(
                    IdentityStep.class,
                    OrderStep.class
            ));

    @Override
    public void apply(final Traversal traversal) {
        boolean done = false;
        while (!done) {
            done = true;
            for (int i = 0; i < traversal.getSteps().size(); i++) {
                final Step step1 = (Step) traversal.getSteps().get(i);
                if (step1 instanceof DedupStep && !((DedupStep) step1).hasUniqueFunction) {
                    for (int j = i; j >= 0; j--) {
                        final Step step2 = (Step) traversal.getSteps().get(j);
                        if (BIJECTIVE_PIPES.stream().filter(c -> c.isAssignableFrom(step2.getClass())).findFirst().isPresent()) {
                            TraversalHelper.removeStep(step1, traversal);
                            TraversalHelper.insertStep(step1, j, traversal);
                            done = false;
                            break;
                        }
                    }
                }
                if (!done)
                    break;
            }
        }
    }

    public static DedupOptimizerStrategy instance() {
        return INSTANCE;
    }
}
