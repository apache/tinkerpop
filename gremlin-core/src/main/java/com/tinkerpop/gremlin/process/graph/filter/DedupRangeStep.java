package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class DedupRangeStep<S> extends FilterStep<S> {

    public final int low;
    public final int high;

    public DedupRangeStep(final Traversal traversal, final int low, final int high) {
        this(traversal, null, low, high);
    }

    public DedupRangeStep(final Traversal traversal, final SFunction<S, ?> uniqueFunction, final int low, final int high) {
        super(traversal);
        final Set<Object> set = new LinkedHashSet<>();
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + "]");
        }
        this.low = low;
        this.high = high;

        final AtomicInteger counter = new AtomicInteger(-1);
        if (null == uniqueFunction) {
            this.setPredicate(holder -> {
                final int c = counter.get() + 1;
                if (c > high && high != -1)
                    throw FastNoSuchElementException.instance();
                return set.add(holder.get()) && counter.incrementAndGet() >= 0 && c >= low && (high == -1 || c <= high);
            });
        } else {
            this.setPredicate(holder -> {
                final int c = counter.get() + 1;
                if (c > high && high != -1)
                    throw FastNoSuchElementException.instance();
                final Object value = uniqueFunction.apply(holder.get());
                return set.add(value) && counter.incrementAndGet() >= 0 && c >= low && (high == -1 || c <= high);
            });
        }
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.low, this.high);
    }
}
