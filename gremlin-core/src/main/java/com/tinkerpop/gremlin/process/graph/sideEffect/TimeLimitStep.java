package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.MapStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Randall Barnhart (random pi)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TimeLimitStep<S> extends MapStep<S, S> {

    private final AtomicLong startTime = new AtomicLong(-1);

    public TimeLimitStep(final Traversal traversal, final long timeLimit) {
        super(traversal);
        super.setFunction(traverser -> {
            if (this.startTime.get() == -1l)
                this.startTime.set(System.currentTimeMillis());
            if ((System.currentTimeMillis() - this.startTime.get()) >= timeLimit)
                throw FastNoSuchElementException.instance();
            return traverser.get();
        });
    }
}
