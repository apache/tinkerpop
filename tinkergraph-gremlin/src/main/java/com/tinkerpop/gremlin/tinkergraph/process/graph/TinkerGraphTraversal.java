package com.tinkerpop.gremlin.tinkergraph.process.graph;

import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect.TinkerGraphStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerGraphStepStrategy;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    static {
        GraphTraversalStrategyRegistry.instance().register(TinkerGraphStepStrategy.instance());
    }

    public TinkerGraphTraversal(final TinkerGraph graph, final Class<? extends Element> elementClass) {
        super(graph);
        this.addStep(new TinkerGraphStep(this, elementClass));
    }

    @Override
    public void prepareForGraphComputer() {
        super.prepareForGraphComputer();
        this.strategies().unregister(TinkerGraphStepStrategy.class);
    }
}
