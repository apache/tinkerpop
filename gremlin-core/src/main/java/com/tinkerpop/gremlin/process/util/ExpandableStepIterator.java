package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExpandableStepIterator<E> implements Iterator<Traverser<E>>, Serializable {

    private ExpandableIterator<Traverser<E>> expander = null;
    private Step<?, E> hostStep = EmptyStep.instance();

    public ExpandableStepIterator(final Step<?, E> hostStep) {
        this.hostStep = hostStep;
    }

    @Override
    public boolean hasNext() {
        return this.expanderHasNext() || this.hostStep.getPreviousStep().hasNext();
    }

    @Override
    public Traverser<E> next() {
        if (this.expanderHasNext())
            return this.expander.next();

        if (this.hostStep.getPreviousStep().hasNext())
            return (Traverser<E>) this.hostStep.getPreviousStep().next();
        else
            return this.expanderNext();
    }

    public void add(final Iterator<E> iterator) {
        if (null == this.expander)
            this.expander = new ExpandableIterator<>();
        this.expander.add((Iterator) iterator);
    }

    @Override
    public String toString() {
        return this.expander.toString();
    }

    public boolean expanderHasNext() {
        return null != this.expander && this.expander.hasNext();
    }

    public Traverser<E> expanderNext() {
        if (null == this.expander) throw FastNoSuchElementException.instance();
        else return this.expander.next();
    }

    public void clear() {
        if (null != this.expander)
            this.expander.clear();
    }

    public class ExpandableIterator<T> implements Iterator<T>, Serializable {

        private final Queue<Iterator<T>> queue = new LinkedList<>();

        public void clear() {
            this.queue.clear();
        }

        @Override
        public boolean hasNext() {
            for (final Iterator<T> itty : this.queue) {
                if (itty.hasNext())
                    return true;
            }
            return false;
        }

        @Override
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

        public String toString() {
            return this.queue.toString();
        }
    }
}
