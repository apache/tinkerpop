package com.tinkerpop.gremlin.process.graph.step.sideEffect;

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
public class TimeLimitStep<S> extends SideEffectStep<S> implements Reversible {

    private final AtomicLong startTime = new AtomicLong(-1);
    public final long timeLimit;

    public TimeLimitStep(final Traversal traversal, final long timeLimit) {
        super(traversal);
        this.timeLimit = timeLimit;
        super.setConsumer(traverser -> {
            if (this.startTime.get() == -1l)
                this.startTime.set(System.currentTimeMillis());
            if ((System.currentTimeMillis() - this.startTime.get()) >= this.timeLimit)
                throw FastNoSuchElementException.instance();
        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.timeLimit);
    }
}
