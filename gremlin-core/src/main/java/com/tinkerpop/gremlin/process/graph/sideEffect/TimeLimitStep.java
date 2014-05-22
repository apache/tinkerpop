package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.MapStep;

import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Randall Barnhart (random pi)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TimeLimitStep<S> extends MapStep<S, S> {

    private final AtomicBoolean done = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Timer timer = new Timer();

    public TimeLimitStep(final Traversal traversal, final long timeLimit) {
        super(traversal);
        super.setFunction(s -> {
            if (!this.started.get()) {
                this.started.set(true);
                this.timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        done.set(true);
                    }
                }, timeLimit);
            }
            if (!done.get()) {
                return s.get();
            } else {
                throw new NoSuchElementException();
            }
        });
    }
}
