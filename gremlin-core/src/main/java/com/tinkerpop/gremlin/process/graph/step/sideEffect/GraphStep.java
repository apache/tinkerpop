package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphStep<E extends Element> extends StartStep<E> implements TraverserSource {

    protected final Class<E> returnClass;
    protected final Object[] ids;
    protected final Graph graph;
    protected Supplier<Iterator<E>> iteratorSupplier;

    public GraphStep(final Traversal traversal, final Graph graph, final Class<E> returnClass, final Object... ids) {
        super(traversal);
        this.graph = graph;
        this.returnClass = returnClass;
        this.ids = ids;
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ?
                this.graph.iterators().vertexIterator(this.ids) :
                this.graph.iterators().edgeIterator(this.ids)));
    }

    public String toString() {
        return 0 == this.ids.length ?
                TraversalHelper.makeStepString(this, returnClass.getSimpleName().toLowerCase()) :
                TraversalHelper.makeStepString(this, returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids));
    }

    public boolean returnsVertices() {
        return Vertex.class.isAssignableFrom(this.returnClass);
    }

    public boolean returnsEdges() {
        return Edge.class.isAssignableFrom(this.returnClass);
    }

    public void setIteratorSupplier(final Supplier<Iterator<E>> iteratorSupplier) {
        this.iteratorSupplier = iteratorSupplier;
    }

    public <G extends Graph> G getGraph(final Class<G> graphClass) {
        return (G) this.graph;
    }

    public Object[] getIds() {
        return this.ids;
    }

    @Override
    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        try {
            this.start = this.iteratorSupplier.get();
            super.generateTraversers(traverserGenerator);
            // TODO: rjbriod - is this catch necessary even with profiling removed?
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
