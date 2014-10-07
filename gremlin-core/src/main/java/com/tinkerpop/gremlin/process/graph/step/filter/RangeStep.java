package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RangeStep<S> extends FilterStep<S> {

    private final long low;
    private final long high;
    private final AtomicLong counter = new AtomicLong(-1l);

    public RangeStep(final Traversal traversal, final long low, final long high) {
        super(traversal);
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + "]");
        }
        this.low = low;
        this.high = high;


        this.setPredicate(traverser -> {
            final long tempCounter = this.counter.get() + traverser.getBulk();
            if ((this.low == -1 || tempCounter >= this.low) && (this.high == -1 || tempCounter <= this.high)) {
                this.counter.set(tempCounter);
                return true;
            } else if (this.high != -1 && tempCounter > this.high) {
                final long differenceCounter = tempCounter - this.high;
                if (traverser.getBulk() > differenceCounter) {
                    traverser.asSystem().setBulk(differenceCounter);
                    this.counter.set(this.counter.get() + differenceCounter);
                    return true;
                } else
                    throw FastNoSuchElementException.instance();
            } else {
                this.counter.set(tempCounter);
                return false;
            }
        });
    }

    @Override
    public void reset() {
        super.reset();
        this.counter.set(-1l);
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
