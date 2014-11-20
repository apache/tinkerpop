package com.tinkerpop.gremlin.hadoop.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import com.tinkerpop.gremlin.hadoop.structure.hdfs.HadoopEdgeIterator;
import com.tinkerpop.gremlin.hadoop.structure.hdfs.HadoopVertexIterator;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopGraphStep<E extends Element> extends GraphStep<E> {

    private final HadoopGraph graph;

    public HadoopGraphStep(final Traversal traversal, final Class<E> returnClass, final HadoopGraph graph) {
        super(traversal, returnClass);
        this.graph = graph;
    }

    @Override
    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        if (PROFILING_ENABLED) TraversalMetrics.start(this);
        try {
            this.start = Vertex.class.isAssignableFrom(this.returnClass) ? new HadoopVertexIterator(this.graph) : new HadoopEdgeIterator(this.graph);
            super.generateTraversers(traverserGenerator);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            if (PROFILING_ENABLED) TraversalMetrics.stop(this);
        }
    }
}
