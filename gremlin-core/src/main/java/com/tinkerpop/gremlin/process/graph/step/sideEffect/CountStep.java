package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountStep<S> extends MapStep<S, Long> implements Bulkable {

    private long bulkCount = 1l;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final AtomicLong counter = new AtomicLong(0l);

    public CountStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> {
            this.counter.set(this.counter.get() + this.bulkCount);
            this.starts.forEachRemaining(previousTraverser -> this.counter.set(this.counter.get() + this.bulkCount));
            return this.counter.get();
        });
    }

    @Override
    protected Traverser<Long> processNextStart() {
        if (!this.done.get()) {
            this.done.set(true);
            if (this.starts.hasNext()) {
                return super.processNextStart();
            } else {
                return new SimpleTraverser<>(0l,this.traversal.sideEffects());
            }
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    @Override
    public void reset() {
        super.reset();
        this.done.set(false);
        this.counter.set(0l);
    }
}
