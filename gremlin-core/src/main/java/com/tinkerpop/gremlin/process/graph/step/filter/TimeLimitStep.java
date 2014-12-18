package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Randall Barnhart (random pi)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TimeLimitStep<S> extends FilterStep<S> implements Reversible {

    private final AtomicLong startTime = new AtomicLong(-1);
    private final long timeLimit;
    private final AtomicBoolean timedOut = new AtomicBoolean(false);


    public TimeLimitStep(final Traversal traversal, final long timeLimit) {
        super(traversal);
        this.timeLimit = timeLimit;
        TimeLimitStep.generatePredicate(this);
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.timeLimit);
    }

    @Override
    public void reset() {
        super.reset();
        this.startTime.set(-1l);
        this.timedOut.set(false);
    }

    public boolean getTimedOut() {
        return this.timedOut.get();
    }

    @Override
    public TimeLimitStep<S> clone() throws CloneNotSupportedException {
        final TimeLimitStep<S> clone = (TimeLimitStep<S>) super.clone();
        clone.timedOut.set(this.timedOut.get());
        clone.startTime.set(this.startTime.get());
        TimeLimitStep.generatePredicate(clone);
        return clone;
    }

    ///////

    public static <S> void generatePredicate(final TimeLimitStep<S> timeLimitStep) {
        timeLimitStep.setPredicate(traverser -> {
            if (timeLimitStep.startTime.get() == -1l)
                timeLimitStep.startTime.set(System.currentTimeMillis());
            if ((System.currentTimeMillis() - timeLimitStep.startTime.get()) >= timeLimitStep.timeLimit) {
                timeLimitStep.timedOut.set(true);
                throw FastNoSuchElementException.instance();
            }
            return true;
        });
    }
}
