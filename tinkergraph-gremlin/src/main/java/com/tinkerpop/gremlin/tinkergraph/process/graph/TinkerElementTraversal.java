package com.tinkerpop.gremlin.tinkergraph.process.graph;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect.TinkerGraphStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerGraphStepStrategy;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerElementTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    private final Class<? extends Element> elementClass;
    private final Object id;

    public TinkerElementTraversal(final Element element, final TinkerGraph graph) {
        super(graph);
        this.elementClass = element.getClass();
        this.id = element.id();
        this.addStep(new StartStep<>(this, element));
    }

    @Override
    public void prepareForGraphComputer() {
        super.prepareForGraphComputer();
        this.strategies().unregister(TinkerGraphStepStrategy.class);  // shouldn't be needed, but doesn't hurt
    }

    @Override
    public GraphTraversal<S, E> submit(final GraphComputer computer) {
        final String label = this.getSteps().get(0).getLabel();
        TraversalHelper.removeStep(0, this);
        if (TraversalHelper.isLabeled(label)) {
            final Step identityStep = new IdentityStep(this);
            identityStep.setLabel(label);
            TraversalHelper.insertStep(identityStep, 0, this);
        }
        TraversalHelper.insertStep(new HasStep(this, new HasContainer(T.id, Compare.eq, this.id)), 0, this);
        TraversalHelper.insertStep(new TinkerGraphStep<>(this, this.elementClass), 0, this);
        return super.submit(computer);
    }
}
