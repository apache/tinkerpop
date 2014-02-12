package com.tinkerpop.gremlin.process.olap.gremlin.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.sideeffect.GroupCountStep;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Traversal traversal) {
        // if traversal has a GroupCountStep, replace it with a Gremlin OLAP GroupCountStep using ReductionMemory
        for (Step step : (List<Step>) traversal.getSteps()) {
            if (step instanceof GroupCountStep) {
                //step.get
            }
        }
    }
}