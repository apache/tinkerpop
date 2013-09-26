package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinStream<S, E, T extends BaseStream>  {

    private final Stream<S> stream;

    public GremlinStream(final Iterable<S> iterable) {
        this.stream = StreamFactory.stream(iterable);
    }

    public T parallel() {
        this.stream.parallel();
        return (T) this;
    }

    public T sequential() {
        this.stream.sequential();
        return (T) this;
    }

    public Spliterator<S> spliterator() {
        return this.stream.spliterator();
    }

    public boolean isParallel() {
        return this.stream.isParallel();
    }

    public void close() {
        this.stream.close();
    }

    public T unordered() {
        return (T) this;
    }

    public T onClose(Runnable runnable) {
        return (T) this;
    }

    public Iterator<S> iterator() {
        return this.stream.iterator();
    }
}
