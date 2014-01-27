package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.GraphQueryBuilder;
import com.tinkerpop.gremlin.oltp.map.GraphQueryPipe;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.optimizers.GraphQueryOptimizer;
import com.tinkerpop.gremlin.util.optimizers.HolderOptimizer;
import com.tinkerpop.gremlin.util.optimizers.IdentityOptimizer;
import com.tinkerpop.gremlin.util.optimizers.VertexQueryOptimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Gremlin<S, E> implements Pipeline<S, E> {

    private final Map<String, Object> variables = new HashMap<>();
    private final List<Pipe> pipes = new ArrayList<>();
    private final List<Optimizer> optimizers = new ArrayList<>();
    private final Graph graph;
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
        Gremlin gremlin = Gremlin.of(EmptyGraph.instance());
        gremlin.addPipe(new IdentityPipe(gremlin));
        return gremlin;

    }

    public static Gremlin<?, ?> of(final Graph graph) {
        return new Gremlin(graph, true);
    }

    public static Gremlin<?, ?> of(final Graph graph, final boolean useDefaultOptimizers) {
        return new Gremlin(graph, useDefaultOptimizers);
    }

    public <T> void set(final String variable, final T t) {
        this.variables.put(variable, t);
    }

    public <T> T get(final String variable) {
        return (T) this.variables.get(variable);
    }

    public Set<String> getVariables() {
        return this.variables.keySet();
    }

    public void registerOptimizer(final Optimizer optimizer) {
        this.optimizers.add(optimizer);
    }

    public List<Optimizer> getOptimizers() {
        return this.optimizers;
    }

    // TODO: Is this good?
    public Gremlin<S, E> with(final Map<String, Object> bindings) {
        bindings.forEach(this.variables::put);
        return this;
    }

    public Gremlin<S, E> with(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            this.variables.put((String) keyValues[i], keyValues[i + 1]);
        }
        return this;
    }

    public Gremlin<Vertex, Vertex> V() {
        this.addPipe(new GraphQueryPipe(this, this.graph, new GraphQueryBuilder(), Vertex.class));
        return (Gremlin<Vertex, Vertex>) this;
    }

    public Gremlin<Edge, Edge> E() {
        this.addPipe(new GraphQueryPipe(this, this.graph, new GraphQueryBuilder(), Edge.class));
        return (Gremlin<Edge, Edge>) this;
    }

    public Gremlin<Vertex, Vertex> v(final Object... ids) {
        this.addPipe(new GraphQueryPipe(this, this.graph, new GraphQueryBuilder().ids(ids), Vertex.class));
        return (Gremlin<Vertex, Vertex>) this;
    }

    public Gremlin<Edge, Edge> e(final Object... ids) {
        this.addPipe(new GraphQueryPipe(this, this.graph, new GraphQueryBuilder().ids(ids), Edge.class));
        return (Gremlin<Edge, Edge>) this;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        ((Pipe<S, ?>) this.pipes.get(0)).addStarts(starts);
    }

    public List<Pipe> getPipes() {
        return this.pipes;
    }

    public <S, E> Pipeline<S, E> addPipe(final Pipe<?, E> pipe) {
        if (this.optimizers.stream()
                .filter(optimizer -> optimizer instanceof Optimizer.StepOptimizer)
                .map(optimizer -> ((Optimizer.StepOptimizer) optimizer).optimize(this, pipe))
                .reduce(true, (a, b) -> a && b)) {

            if (this.pipes.size() > 0) {
                pipe.setPreviousPipe(this.pipes.get(this.pipes.size() - 1));
                this.pipes.get(this.pipes.size() - 1).setNextPipe(pipe);
            }
            this.pipes.add(pipe);
        }
        return (Gremlin<S, E>) this;
    }

    public boolean hasNext() {
        if (this.firstNext) this.finalOptimize();
        return this.pipes.get(this.pipes.size() - 1).hasNext();
    }

    public E next() {
        if (this.firstNext) this.finalOptimize();
        return ((Holder<E>) this.pipes.get(this.pipes.size() - 1).next()).get();
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
                .filter(optimizer -> optimizer instanceof Optimizer.FinalOptimizer)
                .map(optimizer -> ((Optimizer.FinalOptimizer) optimizer).optimize(this)).count();
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && GremlinHelper.areEqual(this, (Iterator) object);
    }

}
