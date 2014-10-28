package com.tinkerpop.gremlin.tinkergraph.process.graph;

import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect.TinkerGraphStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerGraphStepStrategy;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    public TinkerGraphTraversal(final Class<? extends Element> elementClass, final TinkerGraph graph) {
        super(graph);
        this.getStrategies().register(TinkerGraphStepStrategy.instance());
        this.addStep(new TinkerGraphStep<>(this, elementClass));
    }
}
