package com.tinkerpop.gremlin.process.oltp.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.map.IdentityStep;
import com.tinkerpop.gremlin.process.oltp.util.GremlinHelper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Traversal traversal) {
        ((List<Step>) traversal.getSteps()).stream()
                .filter(pipe -> pipe instanceof IdentityStep && !GremlinHelper.isLabeled(pipe))
                .collect(Collectors.<Step>toList())
                .forEach(pipe -> GremlinHelper.removeStep(pipe, traversal));
    }
}
