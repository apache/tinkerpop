package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FlatMapStep<S, E> extends AbstractStep<S, E> {

    public SFunction<Traverser<S>, Iterator<E>> function;
    protected Iterator<Traverser<E>> iterator = null;

    public FlatMapStep(final Traversal traversal) {
        super(traversal);
    }

    public void setFunction(final SFunction<Traverser<S>, Iterator<E>> function) {
        this.function = function;
    }

    protected Traverser<E> processNextStart() {
        while (true) {
            final Traverser<E> traverser = this.getNext();
            if (null != traverser) return traverser;
        }
    }

    protected Traverser<E> getNext() {
        if (null == this.iterator) {
            final Traverser<S> traverser = this.starts.next();
            this.iterator = new FlatMapHolderIterator<>(traverser, this, this.function.apply(traverser));
            return null;
        } else {
            if (this.iterator.hasNext()) {
                return this.iterator.next();
            } else {
                this.iterator = null;
                return null;
            }
        }
    }

    private class FlatMapHolderIterator<A, B> implements Iterator<Traverser<B>> {

        private final Traverser<A> head;
        private final Iterator<B> iterator;
        private final Step step;

        protected FlatMapHolderIterator(final Traverser<A> head, final Step step, final Iterator<B> iterator) {
            this.iterator = iterator;
            this.head = head;
            this.step = step;
        }

        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        public Traverser<B> next() {
            return this.head.makeChild(this.step.getLabel(), this.iterator.next());
        }
    }

    @Override
    public void reset() {
        this.iterator = null;
    }

}
