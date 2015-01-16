package com.tinkerpop.gremlin.tinkergraph.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerElementStepStrategy extends AbstractTraversalStrategy {

    private static final TinkerElementStepStrategy INSTANCE = new TinkerElementStepStrategy();

    private TinkerElementStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD))
            return;

        final StartStep<Element> startStep = (StartStep) TraversalHelper.getStart(traversal);
        if (startStep.startAssignableTo(Vertex.class, Edge.class)) {
            final Element element = ((StartStep<?>) startStep).getStart();
            traversal.removeStep(startStep);
            startStep.getLabel().ifPresent(label -> {
                final Step identityStep = new IdentityStep(traversal);
                identityStep.setLabel(label);
                traversal.addStep(0, identityStep);
            });
            traversal.addStep(0, new GraphStep<>(traversal, EmptyGraph.instance(), element.getClass(), element.id()));
        }
    }

    public static TinkerElementStepStrategy instance() {
        return INSTANCE;
    }

}
