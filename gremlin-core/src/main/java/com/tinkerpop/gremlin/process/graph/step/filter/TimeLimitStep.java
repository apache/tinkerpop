package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Randall Barnhart (random pi)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TimeLimitStep<S> extends FilterStep<S> implements Reversible {

    private final AtomicLong startTime = new AtomicLong(-1);
    private final long timeLimit;

    public TimeLimitStep(final Traversal traversal, final long timeLimit) {
        super(traversal);
        this.timeLimit = timeLimit;
        super.setPredicate(traverser -> {
            if (this.startTime.get() == -1l)
                this.startTime.set(System.currentTimeMillis());
            if ((System.currentTimeMillis() - this.startTime.get()) >= this.timeLimit)
                throw FastNoSuchElementException.instance();
            return true;
        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.timeLimit);
    }

    @Override
    public void reset() {
        super.reset();
        this.startTime.set(-1l);
    }
}
