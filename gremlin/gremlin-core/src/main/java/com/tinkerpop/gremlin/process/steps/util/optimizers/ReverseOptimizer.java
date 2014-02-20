package com.tinkerpop.gremlin.process.steps.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.steps.map.VertexStep;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReverseOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Traversal traversal) {
        ((List<Step>) traversal.getSteps()).stream()
                .filter(step -> step instanceof VertexStep)
                .forEach(step -> {
                    ((VertexStep) step).direction = ((VertexStep) step).direction.opposite();
                });
        ((List<Step>) traversal.getSteps()).stream()
                .filter(step -> step instanceof EdgeVertexStep)
                .forEach(step -> {
                    ((EdgeVertexStep) step).direction = ((EdgeVertexStep) step).direction.opposite();
                });

    }
}
