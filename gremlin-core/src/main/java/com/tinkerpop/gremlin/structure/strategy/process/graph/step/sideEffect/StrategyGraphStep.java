package com.tinkerpop.gremlin.structure.strategy.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.strategy.StrategyEdge;
import com.tinkerpop.gremlin.structure.strategy.StrategyGraph;
import com.tinkerpop.gremlin.structure.strategy.StrategyVertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyGraphStep<E extends Element> extends GraphStep<E> {

    private final GraphTraversal<?, E> graphTraversal;

    public StrategyGraphStep(final Traversal traversal, final StrategyGraph strategyGraph, final Class<E> returnClass, final GraphTraversal<?, E> graphTraversal) {
        super(traversal, strategyGraph, returnClass);
        this.graphTraversal = graphTraversal;
        this.setIteratorSupplier(() -> (Iterator) (Vertex.class.isAssignableFrom(this.returnClass) ?
                new StrategyVertex.StrategyVertexIterator((Iterator) this.graphTraversal, strategyGraph) :
                new StrategyEdge.StrategyEdgeIterator((Iterator) this.graphTraversal, strategyGraph)));
    }
}
