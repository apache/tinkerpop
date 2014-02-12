package com.tinkerpop.gremlin.process.oltp.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.filter.DedupStep;
import com.tinkerpop.gremlin.process.oltp.map.IdentityStep;
import com.tinkerpop.gremlin.process.oltp.map.OrderStep;
import com.tinkerpop.gremlin.process.oltp.util.GremlinHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DedupOptimizer implements Optimizer.FinalOptimizer {

    private static final List<Class> BIJECTIVE_PIPES = new ArrayList<Class>(
            Arrays.asList(
                    IdentityStep.class,
                    OrderStep.class
            ));

    public void optimize(final Traversal traversal) {
        boolean done = false;
        while (!done) {
            done = true;
            for (int i = 0; i < traversal.getSteps().size(); i++) {
                final Step step1 = (Step) traversal.getSteps().get(i);
                if (step1 instanceof DedupStep && !((DedupStep) step1).hasUniqueFunction) {
                    for (int j = i; j >= 0; j--) {
                        final Step step2 = (Step) traversal.getSteps().get(j);
                        if (BIJECTIVE_PIPES.stream().filter(c -> c.isAssignableFrom(step2.getClass())).findFirst().isPresent()) {
                            GremlinHelper.removePipe(step1, traversal);
                            GremlinHelper.insertPipe(step1, j, traversal);
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
}
