package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.pipes.util.HolderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Gremlin<S, E> implements Pipeline<S, E> {

    private final Map<String, Object> variables = new HashMap<>();
    private final List<Pipe<?, ?>> pipes = new ArrayList<>();
    private final List<Optimizer> optimizers = new ArrayList<>();
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

    public <T> void put(final String variable, final T t) {
        this.variables.put(variable, t);
    }

    public <T> Optional<T> get(final String variable) {
        return Optional.ofNullable((T) this.variables.get(variable));
    }

    public void registerOptimizer(final Optimizer optimizer) {
        this.optimizers.add(optimizer);
    }

    public void optimize() {
        this.optimizers.stream()
                .filter(o -> o.getOptimizationRate().equals(Optimizer.Rate.COMPILE_TIME))
                .forEach(o -> o.optimize(this));
    }

    public Gremlin<Vertex, Vertex> V() {
        Objects.requireNonNull(this.graph);
        final Pipe<S, S> pipe = new MapPipe<S, S>(this, s -> s.get());
        this.addPipe(pipe);
        this.addStarts(new HolderIterator(Optional.empty(), pipe, this.graph.query().vertices().iterator()));
        return (Gremlin<Vertex, Vertex>) this;
    }

    public Gremlin<Edge, Edge> E() {
        Objects.requireNonNull(this.graph);
        final Pipe<S, S> pipe = new MapPipe<S, S>(this, s -> s.get());
        this.addPipe(pipe);
        this.addStarts(new HolderIterator(Optional.empty(), pipe, this.graph.query().edges().iterator()));
        return (Gremlin<Edge, Edge>) this;
    }

    public Gremlin<Vertex, Vertex> v(final Object... ids) {
        Objects.requireNonNull(this.graph);
        final Pipe<S, S> pipe = new MapPipe<S, S>(this, s -> s.get());
        this.addPipe(pipe);
        this.addStarts(new HolderIterator(Optional.empty(), pipe, this.graph.query().ids(ids).vertices().iterator()));
        return (Gremlin<Vertex, Vertex>) this;
    }

    public Gremlin<Edge, Edge> e(final Object... ids) {
        Objects.requireNonNull(this.graph);
        final Pipe<S, S> pipe = new MapPipe<S, S>(this, s -> s.get());
        this.addPipe(pipe);
        this.addStarts(new HolderIterator(Optional.empty(), pipe, this.graph.query().ids(ids).edges().iterator()));
        return (Gremlin<Edge, Edge>) this;
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

    public E next() {
        return (E) this.pipes.get(this.pipes.size() - 1).next().get();
    }

    public String toString() {
        return this.getPipes().toString();
    }

}
