package com.tinkerpop.gremlin.tinkergraph.process.graph;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerGraphStepStrategy;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    public TinkerTraversal(final TinkerGraph graph) {
        this.sideEffects().setGraph(graph);
        this.strategies().register(TinkerGraphStepStrategy.instance());
        this.addStep(new StartStep<>(this));
    }

    @Override
    public void prepareForGraphComputer() {
        super.prepareForGraphComputer();
        this.strategies().unregister(TinkerGraphStepStrategy.class);
    }
}
