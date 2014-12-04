package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.LocalTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.step.util.LocalTraversalStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalTraversalStrategy extends AbstractTraversalStrategy {

    private static final LocalTraversalStrategy INSTANCE = new LocalTraversalStrategy();

    private LocalTraversalStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {

        TraversalHelper.getStepsOfClass(LocalTraversalStep.class, traversal).forEach(localTraversalStep -> {
            try {
                final Traversal localTraversal = new DefaultGraphTraversal();
                Step previousStep = localTraversalStep.getPreviousStep();
                while (!(previousStep instanceof LocalTraversal)) {
                    TraversalHelper.insertStep(previousStep.clone(), 0, localTraversal);
                    previousStep = previousStep.getPreviousStep();
                }
                TraversalHelper.insertStep(new StartStep<>(localTraversal, null),0,localTraversal);

                previousStep = localTraversalStep.getPreviousStep();
                while (!(previousStep instanceof LocalTraversal)) {
                    TraversalHelper.removeStep(previousStep, traversal);
                    previousStep = previousStep.getPreviousStep();
                }
                ((LocalTraversal) previousStep).setLocalTraversal(localTraversal);
                TraversalHelper.removeStep(localTraversalStep, traversal);

            } catch (Exception e) {
                e.printStackTrace();
            }


            /*while (!previousStep.equals(EmptyStep.instance()) && !(previousStep instanceof PropertiesStep) && !(previousStep instanceof VertexStep)) {
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
            }*/
        });

    }

    public static LocalTraversalStrategy instance() {
        return INSTANCE;
    }
}
