package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphStep<S extends Element> extends StartStep<S> implements EngineDependent {

    protected final Class<S> returnClass;
    protected final Object[] ids;
    protected final Graph graph;
    protected Supplier<Iterator<S>> iteratorSupplier;

    public GraphStep(final Traversal traversal, final Graph graph, final Class<S> returnClass, final Object... ids) {
        super(traversal);
        this.graph = graph;
        this.returnClass = returnClass;
        this.ids = ids;
        this.setIteratorSupplier(() -> (Iterator<S>) (Vertex.class.isAssignableFrom(this.returnClass) ?
                this.graph.iterators().vertexIterator(this.ids) :
                this.graph.iterators().edgeIterator(this.ids)));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, Arrays.asList(this.ids), this.returnClass.getSimpleName().toLowerCase());
    }

    public boolean returnsVertices() {
        return Vertex.class.isAssignableFrom(this.returnClass);
    }

    public boolean returnsEdges() {
        return Edge.class.isAssignableFrom(this.returnClass);
    }

    public Class<S> getReturnClass() {
        return this.returnClass;
    }

    public void setIteratorSupplier(final Supplier<Iterator<S>> iteratorSupplier) {
        this.iteratorSupplier = iteratorSupplier;
    }

    public <G extends Graph> G getGraph(final Class<G> graphClass) {
        return (G) this.graph;
    }

    public Object[] getIds() {
        return this.ids;
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        if (traversalEngine.equals(TraversalEngine.COMPUTER))
            this.setIteratorSupplier(Collections::emptyIterator);
    }

    @Override
    protected Traverser<S> processNextStart() {
        if (this.first) this.start = this.iteratorSupplier.get();
        return super.processNextStart();
    }
}
