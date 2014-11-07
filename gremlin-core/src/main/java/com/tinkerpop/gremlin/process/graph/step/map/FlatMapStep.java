package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FlatMapStep<S, E> extends AbstractStep<S, E> {

    private Function<Traverser<S>, Iterator<E>> function = null;
    private Iterator<Traverser<E>> iterator = Collections.emptyIterator();

    public FlatMapStep(final Traversal traversal) {
        super(traversal);
    }

    public void setFunction(final Function<Traverser<S>, Iterator<E>> function) {
        this.function = function;
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext())
                return this.iterator.next(); // timer start/finish in next() call
            else {
                final Traverser.Admin<S> traverser = this.starts.next();
                if (PROFILING_ENABLED) TraversalMetrics.start(this);
                this.iterator = new FlatMapTraverserIterator<>(traverser, this.function.apply(traverser));
                if (PROFILING_ENABLED) TraversalMetrics.stop(this);
            }
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.iterator = Collections.emptyIterator();
    }

    private final class FlatMapTraverserIterator<A, B> implements Iterator<Traverser<B>> {

        private final Traverser.Admin<A> head;
        private final Iterator<B> iterator;

        private FlatMapTraverserIterator(final Traverser.Admin<A> head, final Iterator<B> iterator) {
            this.iterator = iterator;
            this.head = head;
        }

        @Override
        public final boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public final Traverser<B> next() {
            if (FlatMapStep.PROFILING_ENABLED) TraversalMetrics.start(FlatMapStep.this);
            final Traverser.Admin<B> traverser = this.head.makeChild(FlatMapStep.this.getLabel(), this.iterator.next());
            if (FlatMapStep.PROFILING_ENABLED) TraversalMetrics.finish(FlatMapStep.this, this.head);
            return traverser;
        }
    }
}
