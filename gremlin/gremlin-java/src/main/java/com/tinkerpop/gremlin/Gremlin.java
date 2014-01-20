package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.GraphQueryBuilder;
import com.tinkerpop.gremlin.pipes.map.GraphQueryPipe;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;
import com.tinkerpop.gremlin.pipes.util.optimizers.GraphQueryOptimizer;
import com.tinkerpop.gremlin.pipes.util.optimizers.HolderOptimizer;
import com.tinkerpop.gremlin.pipes.util.optimizers.IdentityOptimizer;
import com.tinkerpop.gremlin.pipes.util.optimizers.VertexQueryOptimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Gremlin<S, E> implements Pipeline<S, E> {

    private final Map<String, Object> variables = new HashMap<>();
    private final List<Pipe<?, ?>> pipes = new ArrayList<>();
    private final List<Optimizer> optimizers = new ArrayList<>();
    private Graph graph = null;
    private boolean firstNext = true;

    protected Gremlin(final Graph graph, final boolean useDefaultOptimizers) {
        this.graph = graph;
        if (useDefaultOptimizers) {
            this.optimizers.add(new IdentityOptimizer());
            this.optimizers.add(new HolderOptimizer());
            this.optimizers.add(new VertexQueryOptimizer());
            this.optimizers.add(new GraphQueryOptimizer());
        }
    }

    public static Gremlin<?, ?> of() {
        return Gremlin.of(EmptyGraph.instance());
    }

    public static Gremlin<?, ?> of(final Graph graph) {
        return new Gremlin(graph, true);
    }

    public static Gremlin<?, ?> of(final Graph graph, final boolean useDefaultOptimizers) {
        return new Gremlin(graph, useDefaultOptimizers);
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

    public List<Optimizer> getOptimizers() {
        return this.optimizers;
    }

    public Gremlin<Vertex, Vertex> V() {
        final GraphQueryPipe pipe = new GraphQueryPipe<>(this, this.graph, new GraphQueryBuilder(), Vertex.class);
        this.addPipe(pipe);
        return (Gremlin<Vertex, Vertex>) this;
    }

    public Gremlin<Edge, Edge> E() {
        final GraphQueryPipe pipe = new GraphQueryPipe<>(this, this.graph, new GraphQueryBuilder(), Edge.class);
        this.addPipe(pipe);
        return (Gremlin<Edge, Edge>) this;
    }

    public Gremlin<Vertex, Vertex> v(final Object... ids) {
        final GraphQueryPipe pipe = new GraphQueryPipe<>(this, this.graph, new GraphQueryBuilder().ids(ids), Vertex.class);
        this.addPipe(pipe);
        return (Gremlin<Vertex, Vertex>) this;
    }

    public Gremlin<Edge, Edge> e(final Object... ids) {
        final GraphQueryPipe pipe = new GraphQueryPipe<>(this, this.graph, new GraphQueryBuilder().ids(ids), Edge.class);
        this.addPipe(pipe);
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
        if (this.optimizers.stream()
                .filter(o -> o instanceof Optimizer.StepOptimizer)
                .map(o -> ((Optimizer.StepOptimizer) o).optimize(this, pipe))
                .reduce(true, (a, b) -> a && b))
            this.pipes.add(pipe);
        return (P) this;
    }

    public boolean hasNext() {
        this.finalOptimize();
        return this.pipes.get(this.pipes.size() - 1).hasNext();
    }

    public E next() {
        this.finalOptimize();
        return (E) this.pipes.get(this.pipes.size() - 1).next().get();
    }

    public String toString() {
        return this.getPipes().toString();
    }

    private void finalOptimize() {
        if (this.firstNext)
            this.firstNext = false;
        else
            return;

        this.optimizers.stream()
                .filter(o -> o instanceof Optimizer.FinalOptimizer)
                .map(o -> ((Optimizer.FinalOptimizer) o).optimize(this)).count();
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && GremlinHelper.areEqual(this, (Iterator) object);
    }

}
