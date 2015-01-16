package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;

import java.io.Serializable;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public class StepTimer implements StepMetrics, Serializable {
    private long timeNs = 0l;
    private long traversers = 0l;
    private long tempTime = -1l;
    private String label;
    private String name;

    private long count;
    private double percentDuration = -1;

    private StepTimer() {
    }

    public StepTimer(final Step<?,?> step) {
        this.label = step.getLabel().orElse(step.getId());
        this.name = step.toString();

    }

    public StepTimer(final String name, final String label) {
        this.label = label;
        this.name = name;
    }

    public final void start() {
        if (-1 != this.tempTime) {
            throw new IllegalStateException("The timer has already been started. Stop timer before starting timer.");
        }
        this.tempTime = System.nanoTime();
    }

    public final void stop() {
        if (-1 == this.tempTime)
            throw new IllegalStateException("The timer has not been started. Start timer before starting timer");
        this.timeNs = this.timeNs + (System.nanoTime() - this.tempTime);
        this.tempTime = -1;
    }

    public long getTimeNs() {
        return this.timeNs;
    }

    public long getTraversers() {
        return this.traversers;
    }

    public double getTimeMs() {
        return this.timeNs / 1000000.0d;
    }

    public long getCount() {
        return count;
    }

    public String toString() {
        return label + ":" + name + " time(ns):" + this.getTimeNs() + " time(ms):" + this.getTimeMs() + " traversers:" + this.getTraversers() + " count:" + this.getCount();
    }

    public void finish(Traverser.Admin<?> traverser) {
        stop();
        this.traversers++;
        this.count += traverser.bulk();
    }

    public void aggregate(StepTimer timer) {
        this.count += timer.count;
        this.traversers += timer.traversers;
        this.timeNs += timer.timeNs;
    }

    public String getName() {
        return name;
    }

    public Double getPercentageDuration() {
        return this.percentDuration;
    }

    public void setPercentageDuration(final double percentDuration) {
        this.percentDuration = percentDuration;
    }

    public String getLabel() {
        return label;
    }

    public String getShortName(int maxLength) {
        if (name.length() > maxLength)
            return name.substring(0, maxLength - 3) + "...";
        return name;
    }
}
