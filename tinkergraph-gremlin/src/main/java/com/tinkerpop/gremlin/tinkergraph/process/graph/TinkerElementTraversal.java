package com.tinkerpop.gremlin.tinkergraph.process.graph;

import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerElementStepStrategy;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerElementTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    public TinkerElementTraversal(final Element element, final TinkerGraph graph) {
        super(graph);
        this.strategies().register(TinkerElementStepStrategy.instance());
        this.addStep(new StartStep<>(this, element));
    }
}
