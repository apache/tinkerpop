package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;

import java.io.Serializable;

public class StepMetrics implements Serializable {
    private long timeNs = 0l;
    private long counts = 0l;
    private long tempTime = -1l;
    private String label;
    private String name;

    private long bulk;

    public StepMetrics(final Step step) {
        this.label = step.getLabel();
        this.name = step.toString();

    }

    public StepMetrics(StepMetrics timer) {
        this.label = timer.label;
        this.name = timer.name;
    }

    public final void startTimer() {
        if (-1 != this.tempTime) {
            throw new IllegalStateException("The timer has already been started. Stop timer before starting timer.");
        }
        this.tempTime = System.nanoTime();
    }

    public final void stopTimer() {
        if (-1 == this.tempTime)
            throw new IllegalStateException("The timer has not been started. Start timer before starting timer");
        this.timeNs = this.timeNs + (System.nanoTime() - this.tempTime);
        this.tempTime = -1;
    }

    public void incrCounts() {
        this.counts++;
    }

    public long getTimeNs() {
        return this.timeNs;
    }

    public long getCounts() {
        return this.counts;
    }

    public double getTimeMs() {
        return this.timeNs / 1000000.0d;
    }


    public long getBulk() {
        return bulk;
    }

    public String toString() {
        return "Timer [" + label + ":" + name + " time(ns):" + this.getTimeNs() + " time(ms):" + this.getTimeMs() + " counts:" + this.getCounts() + " bulk:" + this.getBulk() + "]";
    }

    public void finish(Traverser.Admin<?> traverser) {
        stopTimer();
        incrCounts();
        incrBulk(traverser.getBulk());
    }

    private void incrBulk(long bulk) {
        this.bulk += bulk;
    }

    public void stop() {
        stopTimer();
    }

    public void aggregate(StepMetrics timer) {
        this.bulk += timer.bulk;
        this.counts += timer.counts;
        this.timeNs += timer.timeNs;
    }

    public String getName() {
        return name;
    }

    public Double getPercentageDuration() {
        // TODO profile
        return 0d;
    }
}
