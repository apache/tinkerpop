package com.tinkerpop.gremlin.structure.strategy.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedEdge;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedGraph;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedVertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyWrappedGraphStep<E extends Element> extends GraphStep<E> {

    private final StrategyWrappedGraph strategyWrappedGraph;
    private final GraphTraversal<?, E> graphTraversal;

    public StrategyWrappedGraphStep(final Traversal traversal, final Class returnClass, final GraphTraversal<?, E> graphTraversal, final StrategyWrappedGraph strategyWrappedGraph) {
        super(traversal, returnClass);
        this.strategyWrappedGraph = strategyWrappedGraph;
        this.graphTraversal = graphTraversal;
    }

    @Override
    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        if (PROFILING_ENABLED) TraversalMetrics.start(this);
        try {
            this.start = Vertex.class.isAssignableFrom(this.returnClass) ?
                    new StrategyWrappedVertex.StrategyWrappedVertexIterator((Iterator) this.graphTraversal, this.strategyWrappedGraph) :
                    new StrategyWrappedEdge.StrategyWrappedEdgeIterator((Iterator) this.graphTraversal, this.strategyWrappedGraph);
            super.generateTraversers(traverserGenerator);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            if (PROFILING_ENABLED) TraversalMetrics.stop(this);
        }
    }
}
