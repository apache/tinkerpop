package com.tinkerpop.gremlin.process.graph.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Traversal traversal) {
        ((List<Step>) traversal.getSteps()).stream()
                .filter(step -> step instanceof IdentityStep && !TraversalHelper.isLabeled(step))
                .collect(Collectors.<Step>toList())
                .forEach(step -> TraversalHelper.removeStep(step, traversal));
    }
}
