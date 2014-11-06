package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.LocalRangeStep;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalRangeStrategy extends AbstractTraversalStrategy {

    private static final LocalRangeStrategy INSTANCE = new LocalRangeStrategy();

    private LocalRangeStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        TraversalHelper.getStepsOfClass(LocalRangeStep.class, traversal).forEach(localRangeStep -> {
            Step previousStep = localRangeStep.getPreviousStep();
            while (!previousStep.equals(EmptyStep.instance()) && !(previousStep instanceof PropertiesStep) && !(previousStep instanceof VertexStep)) {
                previousStep = previousStep.getPreviousStep();
                // TODO: check for not filtering/sideEffect steps and throw an exception?
            }
            if (previousStep instanceof VertexStep) {
                VertexStep vertexStep = (VertexStep) previousStep;
                if (vertexStep.getReturnClass().equals(Edge.class)) {
                    localRangeStep.setDirection(vertexStep.getDirection());
                } else {
                    throw new IllegalStateException("LocalRangeStep must follow a VertexStep that produces edges, not vertices");
                }
            } else if (previousStep instanceof PropertiesStep) {
                // do nothing, all is good
            } else {
                throw new IllegalStateException("LocalRangeStep must follow a VertexStep or PropertiesStep");
            }
        });
    }

    public static LocalRangeStrategy instance() {
        return INSTANCE;
    }
}
