package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public class StepTimer extends StepCounter implements StepMetrics {
    private long timeNs = 0l;
    private long tempTime = -1l;

    private double percentDuration = -1;
    private StepTimer previousStepTimer;

    public StepTimer(final ProfileStep<?> step) {
        super(step);
    }

    public StepTimer(final StepTimer timer) {
        super(timer);
    }

    @Override
    public void start() {
        if (-1 != this.tempTime) {
            throw new IllegalStateException("The timer has already been started. Stop timer before starting timer.");
        }
        this.tempTime = System.nanoTime();
    }

    @Override
    public void stop() {
        if (-1 == this.tempTime)
            throw new IllegalStateException("The timer has not been started. Start timer before starting timer");
        this.timeNs = this.timeNs + (System.nanoTime() - this.tempTime);
        this.tempTime = -1;
    }

    @Override
    public long getTimeNs() {
        if (this.previousStepTimer != null) {
            return this.timeNs - this.previousStepTimer.timeNs;
        }
        return this.timeNs;
    }

    @Override
    public double getTimeMs() {
        return getTimeNs() / 1000000.0d;
    }

    @Override
    public void finish(Traverser.Admin<?> traverser) {
        stop();
        super.finish(traverser);
    }

    @Override
    public void aggregate(StepCounter timer) {
        if (!(timer instanceof StepTimer)) {
            throw new IllegalStateException("Invalid aggregate combination of StepCounter and StepTimer.");
        }
        this.timeNs += ((StepTimer) timer).timeNs;
        super.aggregate(timer);
    }

    @Override
    public Double getPercentageDuration() {
        return this.percentDuration;
    }

    @Override
    public void setPercentageDuration(final double percentDuration) {
        this.percentDuration = percentDuration;
    }

    @Override
    public void setPreviousStepCounter(final StepCounter previousStepCounter) {
        if (!(previousStepCounter instanceof StepTimer)) {
            throw new IllegalStateException("Invalid combination of StepCounter with a StepTimer.");
        }
        this.previousStepTimer = (StepTimer) previousStepCounter;
    }
}
