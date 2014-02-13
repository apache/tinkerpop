package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.steps.AbstractStep;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FlatMapStep<S, E> extends AbstractStep<S, E> {

    protected Function<Holder<S>, Iterator<E>> function;
    protected final Queue<Iterator<Holder<E>>> queue = new LinkedList<>();

    public FlatMapStep(final Traversal traversal, Function<Holder<S>, Iterator<E>> function) {
        super(traversal);
        this.function = function;
    }

    public FlatMapStep(final Traversal traversal) {
        super(traversal);
    }

    public void setFunction(final Function<Holder<S>, Iterator<E>> function) {
        this.function = function;
    }

    protected Holder<E> processNextStart() {
        while (true) {
            final Holder<E> holder = this.getNext();
            if (null != holder) return holder;
        }
    }

    protected Holder<E> getNext() {
        if (this.queue.isEmpty()) {
            final Holder<S> holder = this.starts.next();
            this.queue.add(new FlatMapHolderIterator<>(holder, this, this.function.apply(holder)));
            return null;
        } else {
            final Iterator<Holder<E>> iterator = this.queue.peek();
            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                this.queue.remove();
                return null;
            }
        }
    }

    private class FlatMapHolderIterator<A, B> implements Iterator<Holder<B>> {

        private final Holder<A> head;
        private final Iterator<B> iterator;
        private final Step step;

        protected FlatMapHolderIterator(final Holder<A> head, final Step step, final Iterator<B> iterator) {
            this.iterator = iterator;
            this.head = head;
            this.step = step;
        }

        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        public Holder<B> next() {
            return this.head.makeChild(this.step.getAs(), this.iterator.next());
        }
    }

}
