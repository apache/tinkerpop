package com.tinkerpop.gremlin.process.graph.step.map;

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
    private Traverser.Admin<S> head = null;
    private Iterator<E> iterator = Collections.emptyIterator();

    public FlatMapStep(final Traversal traversal) {
        super(traversal);
    }

    public void setFunction(final Function<Traverser<S>, Iterator<E>> function) {
        this.function = function;
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext()) {
                if (PROFILING_ENABLED) TraversalMetrics.start(FlatMapStep.this);
                final Traverser<E> end = this.head.split(this.iterator.next(), this);
                if (PROFILING_ENABLED) TraversalMetrics.finish(FlatMapStep.this, this.head);
                return end;
            } else {
                this.head = this.starts.next();
                if (PROFILING_ENABLED) TraversalMetrics.start(this);
                this.iterator = this.function.apply(this.head);
                if (PROFILING_ENABLED) TraversalMetrics.stop(this);
            }
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.iterator = Collections.emptyIterator();
    }
}
