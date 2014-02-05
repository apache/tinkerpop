package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.GraphQueryBuilder;
import com.tinkerpop.gremlin.oltp.map.GraphQueryPipe;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.optimizers.AnnotatedListQueryOptimizer;
import com.tinkerpop.gremlin.util.optimizers.DedupOptimizer;
import com.tinkerpop.gremlin.util.optimizers.GraphQueryOptimizer;
import com.tinkerpop.gremlin.util.optimizers.HolderOptimizer;
import com.tinkerpop.gremlin.util.optimizers.IdentityOptimizer;
import com.tinkerpop.gremlin.util.optimizers.SimpleOptimizers;
import com.tinkerpop.gremlin.util.optimizers.VertexQueryOptimizer;
import com.tinkerpop.gremlin.util.structures.LocalMemory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinJ<S, E> implements Pipeline<S, E> {

    private final Memory memory = new LocalMemory();
    private final Optimizers optimizers = new SimpleOptimizers();
    private final List<Pipe> pipes = new ArrayList<>();
    private final Graph graph;
    private boolean firstNext = true;

    protected GremlinJ(final Graph graph, final boolean useDefaultOptimizers) {
        this.graph = graph;
        if (useDefaultOptimizers) {
            this.optimizers.register(new IdentityOptimizer());
            this.optimizers.register(new HolderOptimizer());
            this.optimizers.register(new VertexQueryOptimizer());
            this.optimizers.register(new AnnotatedListQueryOptimizer());
            this.optimizers.register(new GraphQueryOptimizer());
            this.optimizers.register(new DedupOptimizer());
        }
    }

    public static GremlinJ<?, ?> of() {
        GremlinJ gremlin = GremlinJ.of(EmptyGraph.instance());
        gremlin.addPipe(new IdentityPipe(gremlin));
        return gremlin;

    }

    public static GremlinJ<?, ?> of(final Graph graph) {
        return new GremlinJ(graph, true);
    }

    public static GremlinJ<?, ?> of(final Graph graph, final boolean useDefaultOptimizers) {
        return new GremlinJ(graph, useDefaultOptimizers);
    }

    public Optimizers optimizers() {
        return this.optimizers;
    }

    public Memory memory() {
        return this.memory;
    }

    // TODO: Is this good?
    public GremlinJ<S, E> with(final Object... variablesValues) {
        for (int i = 0; i < variablesValues.length; i = i + 2) {
            this.memory().set((String) variablesValues[i], variablesValues[i + 1]);
        }
        return this;
    }

    public GremlinJ<Vertex, Vertex> V() {
        this.addPipe(new GraphQueryPipe(this, this.graph, new GraphQueryBuilder(), Vertex.class));
        return (GremlinJ<Vertex, Vertex>) this;
    }

    public GremlinJ<Edge, Edge> E() {
        this.addPipe(new GraphQueryPipe(this, this.graph, new GraphQueryBuilder(), Edge.class));
        return (GremlinJ<Edge, Edge>) this;
    }

    public GremlinJ<Vertex, Vertex> v(final Object... ids) {
        this.addPipe(new GraphQueryPipe(this, this.graph, new GraphQueryBuilder().ids(ids), Vertex.class));
        return (GremlinJ<Vertex, Vertex>) this;
    }

    public GremlinJ<Edge, Edge> e(final Object... ids) {
        this.addPipe(new GraphQueryPipe(this, this.graph, new GraphQueryBuilder().ids(ids), Edge.class));
        return (GremlinJ<Edge, Edge>) this;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        ((Pipe<S, ?>) this.pipes.get(0)).addStarts(starts);
    }

    public List<Pipe> getPipes() {
        return this.pipes;
    }

    public <S, E> Pipeline<S, E> addPipe(final Pipe<?, E> pipe) {
        if (this.optimizers.get().stream()
                .filter(optimizer -> optimizer instanceof Optimizer.StepOptimizer)
                .map(optimizer -> ((Optimizer.StepOptimizer) optimizer).optimize(this, pipe))
                .reduce(true, (a, b) -> a && b)) {

            if (this.pipes.size() > 0) {
                pipe.setPreviousPipe(this.pipes.get(this.pipes.size() - 1));
                this.pipes.get(this.pipes.size() - 1).setNextPipe(pipe);
            }
            this.pipes.add(pipe);
        }
        return (GremlinJ<S, E>) this;
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
        if (!this.firstNext) return;

        this.optimizers.doFinalOptimizers(this);
        this.firstNext = false;
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && GremlinHelper.areEqual(this, (Iterator) object);
    }

}
