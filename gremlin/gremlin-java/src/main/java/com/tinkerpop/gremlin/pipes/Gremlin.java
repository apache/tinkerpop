package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.HolderIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Gremlin<S, E> implements GremlinPipeline<S, E> {

    private final List<Pipe> pipes = new ArrayList<>();
    private Map<String, Integer> asIndex = new HashMap<>();

    public Gremlin(final Iterator<S> starts) {
        this.setStarts(new HolderIterator(starts));
    }

    public Gremlin(final Iterable starts) {
        this(starts.iterator());
    }

    public Pipe setStarts(final Iterator<Holder<S>> starts) {
        this.pipes.add(0, new FilterPipe<S>(s -> true).setStarts(starts));
        return this;
    }

    public void addStart(final Holder<S> start) {

    }

    public Gremlin addPipe(final Pipe pipe) {
        pipe.setStarts(this.lastPipe());
        this.pipes.add(pipe);
        return this;
    }

    public Pipe lastPipe() {
        return this.pipes.get(this.pipes.size() - 1);
    }

    /*public GremlinPipeline as(final String key) {
        if (this.asIndex.containsKey(key))
            throw new IllegalStateException("The named pipe already exists");
        this.asIndex.put(key, pipes.size() - 1);
        return this;
    }

    public GremlinPipeline back(final String key) {
        return this.addPipe(new MapPipe<>(o -> this.pipes.get(this.asIndex.get(key)).getCurrentEnd()));
    }

    public GremlinPipeline loop(final String key, final Predicate<Object> whilePredicate, final Predicate<Object> emitPredicate) {
        return this.addPipe(new MapPipe<>(o -> {
            while (true) {
                if (whilePredicate.test(o))
                    this.pipes.get(this.asIndex.get(key)).addStart(new Holder(o));
                if (emitPredicate.test(o))
                    return o;
            }
        }));
    }*/
}
