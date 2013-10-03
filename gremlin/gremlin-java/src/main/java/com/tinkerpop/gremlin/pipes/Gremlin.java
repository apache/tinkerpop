package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.HolderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Gremlin<S, E> implements GremlinPipeline<S, E> {

    private final List<Pipe> pipes = new ArrayList<>();
    private Pipe<?, E> lastPipe;
    private Graph graph = null;

    public Gremlin(final Graph graph) {
        this.graph = graph;
    }

    public Gremlin(final Iterator<S> starts) {
        this.addStarts(new HolderIterator<>(this, starts));
    }

    public Gremlin(final Iterable<S> starts) {
        this(starts.iterator());
    }

    public static Gremlin of() {
        return new Gremlin(Collections.emptyIterator());
    }

    public static Gremlin of(final Graph graph) {
        return new Gremlin(graph);
    }

    public Gremlin V() {
        this.addStarts(new HolderIterator(this, this.graph.query().vertices().iterator()));
        return this;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        if (this.pipes.size() > 0) {
            this.pipes.get(0).addStarts(starts);
        } else {
            final Pipe<S, S> pipe = new FilterPipe<S>(this, s -> true);
            pipe.addStarts(starts);
            this.pipes.add(pipe);
            this.lastPipe = (Pipe) pipe;
        }
    }

    public List<Pipe> getPipes() {
        return this.pipes;
    }

    public <P extends Pipeline> P addPipe(final Pipe pipe) {
        pipe.addStarts(this.lastPipe);
        this.lastPipe = pipe;
        this.pipes.add(pipe);
        return (P) this;
    }

    public void setName(final String name) {

    }

    public String getName() {
        return "gremlin";
    }

    public boolean hasNext() {
        return this.lastPipe.hasNext();
    }

    public Holder<E> next() {
        return this.lastPipe.next();
    }

}
