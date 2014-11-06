package com.tinkerpop.gremlin.process.traversers;


import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.referenced.ReferencedElement;
import com.tinkerpop.gremlin.structure.util.referenced.ReferencedFactory;
import com.tinkerpop.gremlin.structure.util.referenced.ReferencedProperty;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimpleTraverser<T> implements Traverser<T>, Traverser.Admin<T> {

    private static final String PATH_ERROR_MESSAGE = "Path tracking is not supported by this Traverser: " + SimpleTraverser.class;

    protected T t;
    protected String future = HALT;
    protected short loops = 0;
    protected transient Traversal.SideEffects sideEffects;
    protected long bulk = 1l;

    protected SimpleTraverser() {

    }

    public SimpleTraverser(final T t, final Traversal.SideEffects sideEffects) {
        this.t = t;
        this.sideEffects = sideEffects;
    }

    @Override
    public boolean hasPath() {
        return false;
    }

    @Override
    public T get() {
        return this.t;
    }

    @Override
    public Traversal.SideEffects sideEffects() {
        return this.sideEffects;
    }

    @Override
    public void set(final T t) {
        this.t = t;
    }

    @Override
    public String getFuture() {
        return this.future;
    }

    @Override
    public void setFuture(final String label) {
        this.future = label;
    }

    @Override
    public Path path() {
        throw new IllegalStateException(PATH_ERROR_MESSAGE);
    }

    @Override
    public void setPath(final Path path) {
        throw new IllegalStateException(PATH_ERROR_MESSAGE);
    }

    @Override
    public short loops() {
        return this.loops;
    }

    @Override
    public void incrLoops() {
        this.loops++;
    }

    @Override
    public void resetLoops() {
        this.loops = 0;
    }

    public void setBulk(final long count) {
        this.bulk = count;
    }

    public long bulk() {
        return this.bulk;
    }

    @Override
    public <R> SimpleTraverser<R> makeChild(final String label, final R r) {
        final SimpleTraverser<R> traverser = new SimpleTraverser<>(r, this.sideEffects);
        traverser.future = this.future;
        traverser.loops = this.loops;
        traverser.bulk = this.bulk;
        return traverser;
    }

    @Override
    public SimpleTraverser<T> makeSibling() {
        final SimpleTraverser<T> traverser = new SimpleTraverser<>(this.t, this.sideEffects);
        traverser.future = this.future;
        traverser.loops = this.loops;
        traverser.bulk = this.bulk;
        return traverser;
    }

    @Override
    public void setSideEffects(final Traversal.SideEffects sideEffects) {
        this.sideEffects = sideEffects;
    }

    @Override
    public String toString() {
        return t.toString();
    }

    @Override
    public int hashCode() {
        return this.t.hashCode() + this.future.hashCode() + this.loops;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof SimpleTraverser
                && ((SimpleTraverser) object).get().equals(this.t)
                && ((SimpleTraverser) object).getFuture().equals(this.getFuture())
                && ((SimpleTraverser) object).loops() == this.loops();
    }

    @Override
    public SimpleTraverser<T> detach() {
        if (this.t instanceof Element) {
            this.t = (T) ReferencedFactory.detach((Element) this.t);
        } else if (this.t instanceof Property) {
            this.t = (T) ReferencedFactory.detach((Property) this.t);
        } else if (this.t instanceof Path) {
            this.t = (T) ReferencedFactory.detach((Path) this.t);
        }
        return this;
    }

    @Override
    public SimpleTraverser<T> attach(final Vertex vertex) {
        if (this.t instanceof ReferencedElement) {
            this.t = (T) ReferencedFactory.attach((ReferencedElement) this.t, vertex);
        } else if (this.t instanceof ReferencedProperty) {
            this.t = (T) ReferencedFactory.attach((ReferencedProperty) this.t, vertex);
        }
        // you do not want to attach a path because it will reference graph objects not at the current vertex
        this.sideEffects = TraversalVertexProgram.getLocalSideEffects(vertex);
        return this;
    }

    /*
     @Override
    public SimpleTraverser<T> deflate() {
        if (this.t instanceof Vertex) {
            this.t = (T) DetachedVertex.detach((Vertex) this.t);
        } else if (this.t instanceof Edge) {
            this.t = (T) DetachedEdge.detach((Edge) this.t);
        } else if (this.t instanceof VertexProperty) {
            this.t = (T) DetachedVertexProperty.detach((VertexProperty) this.t);
        } else if (this.t instanceof Property) {
            this.t = (T) DetachedProperty.detach((Property) this.t);
        } else if (this.t instanceof Path) {
            this.t = (T) DetachedPath.detach((Path) this.t);
        }
        this.dropSideEffects();
        return this;
    }

    @Override
    public SimpleTraverser<T> inflate(final Vertex vertex, final Traversal traversal) {
        if (this.t instanceof DetachedVertex) {
            this.t = (T) ((DetachedVertex) this.t).attach(vertex);
        } else if (this.t instanceof DetachedEdge) {
            this.t = (T) ((DetachedEdge) this.t).attach(vertex);
        } else if (this.t instanceof DetachedVertexProperty) {
            this.t = (T) ((DetachedVertexProperty) this.t).attach(vertex);
        } else if (this.t instanceof DetachedProperty) {
            this.t = (T) ((DetachedProperty) this.t).attach(vertex);
        }
        // you do not want to attach a path because it will reference graph objects not at the current vertex
        this.sideEffects = traversal.sideEffects();
        return this;
    }
     */
}
