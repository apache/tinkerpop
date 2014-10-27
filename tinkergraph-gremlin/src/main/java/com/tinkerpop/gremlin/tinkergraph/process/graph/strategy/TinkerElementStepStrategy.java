package com.tinkerpop.gremlin.tinkergraph.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect.TinkerGraphStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerElementStepStrategy extends AbstractTraversalStrategy implements TraversalStrategy.NoDependencies {

    private static final TinkerElementStepStrategy INSTANCE = new TinkerElementStepStrategy();

    private TinkerElementStepStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD))
            return;

        final StartStep<Element> startStep = (StartStep) TraversalHelper.getStart(traversal);
        if (startStep.startAssignableTo(Vertex.class, Edge.class)) {
            final Element element = ((StartStep<?>) startStep).getStart();
            final String label = startStep.getLabel();
            TraversalHelper.removeStep(startStep, traversal);
            if (TraversalHelper.isLabeled(label)) {
                final Step identityStep = new IdentityStep(traversal);
                identityStep.setLabel(label);
                TraversalHelper.insertStep(identityStep, 0, traversal);
            }
            TraversalHelper.insertStep(new HasStep(traversal, new HasContainer(T.id, Compare.eq, element.id())), 0, traversal);
            TraversalHelper.insertStep(new TinkerGraphStep<>(traversal, element.getClass()), 0, traversal);
        }
    }

    public static TinkerElementStepStrategy instance() {
        return INSTANCE;
    }
}
