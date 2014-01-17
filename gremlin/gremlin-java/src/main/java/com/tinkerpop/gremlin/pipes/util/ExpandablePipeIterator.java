package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.Pipe;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExpandablePipeIterator<T> implements Iterator<T> {

    private final ExpandableIterator<T> expander = new ExpandableIterator<>();
    private Pipe<?, T> pipe;

    @SafeVarargs
    public ExpandablePipeIterator(final Iterator<T>... iterators) {
        Stream.of(iterators).forEach(i -> this.add(i));
    }

    public boolean hasNext() {
        return (null != this.pipe && this.pipe.hasNext()) || this.expander.hasNext();
    }

    public T next() {
        if (null != this.pipe && this.pipe.hasNext())
            return (T) this.pipe.next();
        else
            return this.expander.next();
    }

    public void add(final Iterator<T> iterator) {
        if (iterator instanceof Pipe)
            this.pipe = (Pipe) iterator;
        else
            this.expander.add(iterator);
    }

    public class ExpandableIterator<T> implements Iterator<T> {

        private final Queue<Iterator<T>> queue;

        @SafeVarargs
        public ExpandableIterator(final Iterator<T>... iterators) {
            this.queue = new LinkedList<>();
            for (final Iterator<T> iterator : iterators) {
                this.queue.add(iterator);
            }
        }

        public boolean hasNext() {
            for (final Iterator<T> itty : this.queue) {
                if (itty.hasNext())
                    return true;
            }
            return false;
        }

        public T next() {
            while (true) {
                final Iterator<T> itty = this.queue.element();
                if (null != itty && itty.hasNext()) return itty.next();
                else this.queue.remove();
            }
        }

        public void add(final Iterator<T> iterator) {
            this.queue.add(iterator);
        }
    }
}
