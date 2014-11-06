package com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.giraph.hdfs.GiraphEdgeIterator;
import com.tinkerpop.gremlin.giraph.hdfs.GiraphVertexIterator;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphStep<E extends Element> extends GraphStep<E> {

    private final GiraphGraph graph;

    public GiraphGraphStep(final Traversal traversal, final Class<E> returnClass, final GiraphGraph graph) {
        super(traversal, returnClass);
        this.graph = graph;
    }

    @Override
    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        if (PROFILING_ENABLED) TraversalMetrics.start(this);
        try {
            this.start = Vertex.class.isAssignableFrom(this.returnClass) ? new GiraphVertexIterator(this.graph) : new GiraphEdgeIterator(this.graph);
            super.generateTraversers(traverserGenerator);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            if (PROFILING_ENABLED) TraversalMetrics.stop(this);
        }
    }
}
