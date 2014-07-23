package com.tinkerpop.gremlin.tinkergraph.process.graph;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerGraphStepStrategy;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    public TinkerTraversal(final TinkerGraph graph) {
        this.memory().set(Graph.Key.hidden("g"), graph);
        this.strategies().register(new TinkerGraphStepStrategy());
        this.addStep(new StartStep<>(this));
    }

    public GraphTraversal<S, E> submit(final GraphComputer computer) {
        TinkerHelper.prepareTraversalForComputer(this);
        return super.submit(computer);
    }
}
