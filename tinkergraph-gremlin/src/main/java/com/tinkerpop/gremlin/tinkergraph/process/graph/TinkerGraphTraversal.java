package com.tinkerpop.gremlin.tinkergraph.process.graph;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect.TinkerGraphStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerGraphStepStrategy;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    public TinkerGraphTraversal(final TinkerGraph graph, final Class<? extends Element> elementClass) {
        super(graph);
        this.strategies().register(TinkerGraphStepStrategy.instance());
        this.addStep(new TinkerGraphStep(this, elementClass, graph));
    }

    public GraphTraversal<S, E> submit(final GraphComputer computer) {
        TinkerHelper.prepareTraversalForComputer(this);
        return super.submit(computer);
    }
}
