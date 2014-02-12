package com.tinkerpop.gremlin.process.oltp.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.GremlinHelper;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RangeStep<S> extends FilterStep<S> {

    // TODO: May need to make AtomicInteger
    public int low;
    public int high;

    public RangeStep(final Traversal traversal, final int low, final int high) {
        super(traversal);
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + "]");
        }
        this.low = low;
        this.high = high;

        final AtomicInteger counter = new AtomicInteger(-1);
        this.setPredicate(holder -> {
            counter.incrementAndGet();
            if ((this.low == -1 || counter.get() >= this.low) && (this.high == -1 || counter.get() <= this.high))
                return true;
            else if (this.high != -1 && counter.get() > this.high)
                throw FastNoSuchElementException.instance();
            else
                return false;
        });
    }

    public String toString() {
        return GremlinHelper.makeStepString(this, this.low, this.high);
    }
}
