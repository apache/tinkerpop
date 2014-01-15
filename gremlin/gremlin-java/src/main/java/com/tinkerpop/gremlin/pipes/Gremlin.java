package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.pipes.util.HolderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Gremlin<S, E> implements Pipeline<S, E> {

    private final List<Pipe<?, ?>> pipes = new ArrayList<>();
    private Graph graph = null;
    private boolean trackPaths;

    public Gremlin(final Graph graph) {
        this.graph = graph;
    }

    private Gremlin(final Iterator<S> starts) {
        final Pipe<S, S> pipe = new MapPipe<S, S>(this, s -> s.get());
        this.addPipe(pipe);
        this.addStarts(new HolderIterator<>(Optional.empty(), pipe, starts));
    }

    public static Gremlin<?, ?> of() {
        return new Gremlin<>(Collections.emptyIterator());
    }

    public static Gremlin<?, ?> of(final Graph graph) {
        return new Gremlin(graph);
    }

    public Gremlin<Vertex, Vertex> V() {
        Objects.requireNonNull(this.graph);
        final Pipe<S, S> pipe = new MapPipe<S, S>(this, s -> s.get());
        this.addPipe(pipe);
        this.addStarts(new HolderIterator(Optional.empty(), pipe, this.graph.query().vertices().iterator()));
        return (Gremlin<Vertex, Vertex>) this;
    }

    public Gremlin<Vertex, Vertex> v(final Object... ids) {
        Objects.requireNonNull(this.graph);
        final Pipe<S, S> pipe = new MapPipe<S, S>(this, s -> s.get());
        this.addPipe(pipe);
        this.addStarts(new HolderIterator(Optional.empty(), pipe, this.graph.query().ids(ids).vertices().iterator()));
        return (Gremlin<Vertex, Vertex>) this;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        ((Pipe<S, ?>) this.pipes.get(0)).addStarts(starts);
    }

    public List<Pipe<?, ?>> getPipes() {
        return this.pipes;
    }

    public <P extends Pipeline> P addPipe(final Pipe pipe) {
        if (this.pipes.size() > 0)
            pipe.addStarts(this.pipes.get(this.pipes.size() - 1));
        this.pipes.add(pipe);
        return (P) this;
    }

    public void setAs(final String name) {
        //this.pipes.get(0).setAs(name);
    }

    public String getAs() {
        return "gremlin";
        // return this.pipes.get(0).getAs();
    }

    public Gremlin<S, E> trackPaths(final boolean trackPaths) {
        this.trackPaths = trackPaths;
        return this;
    }

    public boolean getTrackPaths() {
        return this.trackPaths;
    }

    public boolean hasNext() {
        return this.pipes.get(this.pipes.size() - 1).hasNext();
    }

    public Holder<E> next() {
        return (Holder<E>) this.pipes.get(this.pipes.size() - 1).next();
    }

    public String toString() {
        return this.getPipes().toString();
    }

}
