package com.tinkerpop.gremlin.pipes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Gremlin<S, E> implements GremlinPipeline<S, E> {

    private final List<Pipe> pipes = new ArrayList<>();
    private Map<String, Integer> asIndex = new HashMap<>();

    public Gremlin(final Iterator<S> starts) {
        this.setStarts(starts);
    }

    public Gremlin(final Iterable<S> starts) {
        this(starts.iterator());
    }

    public Pipe<S, E> setStarts(final Iterator<S> starts) {
        this.pipes.add(0, new FilterPipe<S>(s -> true).setStarts(starts));
        return this;
    }

    public void addStart(final S start) {

    }

    public <T> Gremlin<S, T> addPipe(final Pipe pipe) {
        pipe.setStarts(this.lastPipe());
        this.pipes.add(pipe);
        return (Gremlin<S, T>) this;
    }

    public Pipe<?, E> lastPipe() {
        return this.pipes.get(this.pipes.size() - 1);
    }

    public GremlinPipeline<S, E> as(final String key) {
        if (this.asIndex.containsKey(key))
            throw new IllegalStateException("The named pipe already exists");
        this.asIndex.put(key, pipes.size() - 1);
        return this;
    }

    public GremlinPipeline<S, ?> back(final String key) {
        return this.addPipe(new MapPipe<Object, Object>(o -> this.pipes.get(this.asIndex.get(key)).getCurrentEnd()));
    }

    public GremlinPipeline<S, ?> loop(final String key, final Predicate<Object> whilePredicate, final Predicate<Object> emitPredicate) {
        return this.addPipe(new MapPipe<Object, Object>(o -> {
            while (true) {
                if (whilePredicate.test(o))
                    this.pipes.get(this.asIndex.get(key)).addStart(o);
                if (emitPredicate.test(o))
                    return o;
            }
        }));
    }
}
