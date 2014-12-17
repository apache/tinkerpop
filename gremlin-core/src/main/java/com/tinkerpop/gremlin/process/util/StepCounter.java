package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;

import java.io.Serializable;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public class StepCounter implements StepMetrics, Serializable {
    private long traversers = 0l;
    private String label;
    private String name;

    private long count;

    private StepCounter() {
    }

    public StepCounter(final ProfileStep<?> step) {
        this.label = step.getLabel();
        this.name = step.getEventName();
    }

    public StepCounter(final StepCounter timer) {
        this.label = timer.label;
        this.name = timer.name;
    }


    public void start() {
        // no op for StepCounter
    }

    public void stop() {
        // no op for StepCounter
    }

    public long getTimeNs() {
        return 0;
    }

    public long getTraversers() {
        return this.traversers;
    }

    public double getTimeMs() {
        return 0;
    }

    public long getCount() {
        return count;
    }

    public String toString() {
        return label + ":" + name + " time(ns):" + this.getTimeNs() + " time(ms):" + this.getTimeMs() + " traversers:" + this.getTraversers() + " count:" + this.getCount();
    }

    public void finish(Traverser.Admin<?> traverser) {
        this.traversers++;
        this.count += traverser.bulk();
    }

    public void aggregate(StepCounter timer) {
        this.count += timer.count;
        this.traversers += timer.traversers;
    }

    public String getName() {
        return name;
    }

    public Double getPercentageDuration() {
        return 0.0;
    }

    public void setPercentageDuration(final double percentDuration) {
        // no op for StepCounter
    }

    public String getLabel() {
        return label;
    }

    public <S extends StepCounter> void setPreviousStepCounter(final S previousStepCounter) {
        // no op for StepCounter
    }
}
