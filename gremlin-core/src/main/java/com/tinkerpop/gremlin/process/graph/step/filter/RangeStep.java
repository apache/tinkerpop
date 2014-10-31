package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Ranging;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RangeStep<S> extends FilterStep<S> implements Ranging {

    private final long low;
    private final long high;
    private final AtomicLong counter = new AtomicLong(0l);

    public RangeStep(final Traversal traversal, final long low, final long high) {
        super(traversal);
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + "]");
        }
        this.low = low;
        this.high = high;


        this.setPredicate(traverser -> {
            if (this.high != -1 && this.counter.get() >= this.high) {
                throw FastNoSuchElementException.instance();
            }

            long avail = traverser.bulk();
            if (this.counter.get() + avail <= this.low) {
                // Will not surpass the low w/ this traverser. Skip and filter the whole thing.
                this.counter.getAndAdd(avail);
                return false;
            }

            // Skip for the low and trim for the high. Both can happen at once.

            long toSkip = 0;
            if (this.counter.get() < this.low) {
                toSkip = this.low - this.counter.get();
            }

            long toTrim = 0;
            if (this.high != -1 && this.counter.get() + avail >= this.high) {
                toTrim = this.counter.get() + avail - this.high;
            }

            long toEmit = avail - toSkip - toTrim;
            this.counter.getAndAdd(toSkip + toEmit);
            traverser.asAdmin().setBulk(toEmit);

            return true;
        });
    }

    @Override
    public void reset() {
        super.reset();
        this.counter.set(0l);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.low, this.high);
    }

    public long getLowRange() {
        return this.low;
    }

    public long getHighRange() {
        return this.high;
    }
}
