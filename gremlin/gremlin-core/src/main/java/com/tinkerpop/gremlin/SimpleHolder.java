package com.tinkerpop.gremlin;


import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.blueprints.util.micro.MicroEdge;
import com.tinkerpop.blueprints.util.micro.MicroProperty;
import com.tinkerpop.blueprints.util.micro.MicroVertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimpleHolder<T> implements Holder<T> {

    private static final String PATH_ERROR_MESSAGE = "Path tracking is not supported by this Holder: " + SimpleHolder.class;

    protected T t;
    protected String future = NO_FUTURE;
    protected int loops = 0;

    public SimpleHolder(final T t) {
        this.t = t;
    }

    public T get() {
        return this.t;
    }

    public void set(final T t) {
        this.t = t;
    }

    public String getFuture() {
        return this.future;
    }

    public void setFuture(final String as) {
        this.future = as;
    }

    public Path getPath() {
        throw new IllegalStateException(PATH_ERROR_MESSAGE);
    }

    public void setPath(final Path path) {
        throw new IllegalStateException(PATH_ERROR_MESSAGE);
    }

    public int getLoops() {
        return this.loops;
    }

    public void incrLoops() {
        this.loops++;
    }

    public <R> SimpleHolder<R> makeChild(final String as, final R r) {
        final SimpleHolder<R> holder = new SimpleHolder<>(r);
        holder.future = this.future;
        holder.loops = this.loops;
        return holder;
    }

    public SimpleHolder<T> makeSibling() {
        final SimpleHolder<T> holder = new SimpleHolder<>(this.t);
        holder.future = this.future;
        holder.loops = this.loops;
        return holder;
    }

    public String toString() {
        return t.toString();
    }

    public int hashCode() {
        return this.t.hashCode();
    }

    public Holder<T> deflate() {
        if (this.t instanceof Vertex) {
            this.t = (T) new MicroVertex((Vertex) this.t);
        } else if (this.t instanceof Edge) {
            this.t = (T) new MicroEdge((Edge) this.t);
        } else if (this.t instanceof Property) {
            this.t = (T) new MicroProperty((Property) this.t);
        }
        return this;
    }

    public Holder<T> inflate(final Vertex vertex) {
        if (this.t instanceof MicroVertex) {
            this.t = (T) vertex;
        } else if (this.t instanceof MicroEdge) {
            final String label = ((MicroEdge) this.t).getLabel();
            final Object id = ((MicroEdge) this.t).getId();
            this.t = (T) StreamFactory.stream(vertex.query().direction(Direction.OUT).labels(label).edges())
                    .filter(e -> e.getId().equals(id))
                    .findFirst().get();
        } else if (this.t instanceof MicroProperty) {
            final MicroProperty microProperty = (MicroProperty) this.t;
            if (microProperty.getElement() instanceof Vertex) {
                this.t = (T) vertex.getProperty(microProperty.getKey());
            } else {
                final String label = microProperty.getElement().getLabel();
                final Object id = microProperty.getElement().getId();
                this.t = (T) StreamFactory.stream(vertex.query().direction(Direction.OUT).labels(label).edges())
                        .filter(e -> e.getId().equals(id))
                        .findFirst()
                        .get()
                        .getProperty(microProperty.getKey());
            }
        }
        return this;
    }

    public boolean equals(final Object object) {
        if (object instanceof SimpleHolder)
            return this.t.equals(((SimpleHolder) object).get());
        else
            return false;
    }
}
