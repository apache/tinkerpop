package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;

import java.util.Iterator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FlatMapStep<S, E> extends AbstractStep<S, E> {

    private Function<Traverser<S>, Iterator<E>> function = null;
    private Iterator<Traverser<E>> iterator = null;

    public FlatMapStep(final Traversal traversal) {
        super(traversal);
    }

    public void setFunction(final Function<Traverser<S>, Iterator<E>> function) {
        this.function = function;
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            final Traverser<E> traverser = this.getNext();
            if (null != traverser) return traverser;
        }
    }

    protected Traverser<E> getNext() {
        if (null == this.iterator) {
            final Traverser.Admin<S> traverser = this.starts.next();
            if (this.isProfilingEnabled) TraversalMetrics.start(this, traverser);
            this.iterator = new FlatMapTraverserIterator<>(traverser, this, this.function.apply(traverser));
            if (this.isProfilingEnabled) TraversalMetrics.stop(this, traverser);
            return null;
        } else {
            if (this.iterator.hasNext()) {
                return this.iterator.next(); // timer start/finish in next() call
            } else {
                this.iterator = null;
                return null;
            }
        }
    }

    private class FlatMapTraverserIterator<A, B> implements Iterator<Traverser<B>> {

        private final Traverser.Admin<A> head;
        private final Iterator<B> iterator;
        private final Step step;

        protected FlatMapTraverserIterator(final Traverser.Admin<A> head, final Step step, final Iterator<B> iterator) {
            this.iterator = iterator;
            this.head = head;
            this.step = step;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Traverser<B> next() {
            if (FlatMapStep.this.isProfilingEnabled) TraversalMetrics.start(FlatMapStep.this, this.head);
            Traverser.Admin<B> ret = this.head.makeChild(this.step.getLabel(), this.iterator.next());
            if (FlatMapStep.this.isProfilingEnabled) TraversalMetrics.finish(FlatMapStep.this, this.head);
            return ret;
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.iterator = null;
    }

}
