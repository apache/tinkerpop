package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class GlobalMetrics implements Serializable {
    Map<String, StepMetrics> timers = new LinkedHashMap<String, StepMetrics>();

    private void startInternal(Step<?, ?> step) {
        StepMetrics stepMetrics = timers.get(step.getLabel());
        if (null == stepMetrics) {
            stepMetrics = new StepMetrics(step);
            timers.put(step.getLabel(), stepMetrics);
        }
        stepMetrics.startTimer();
    }

    public static final void stop(Step<?, ?> step, Traverser.Admin<?> traverser) {
        traverser.getSideEffects().<GlobalMetrics>get(ProfileStep.METRICS_KEY).stopInternal(step);
    }

    public static final void finish(Step<?, ?> step, Traverser.Admin<?> traverser) {
        traverser.getSideEffects().<GlobalMetrics>get(ProfileStep.METRICS_KEY).finishInternal(step, traverser);
    }

    private void stopInternal(Step<?, ?> step) {
        timers.get(step.getLabel()).stop();
    }


    private void finishInternal(Step<?, ?> step, Traverser.Admin<?> traverser) {
        timers.get(step.getLabel()).finish(traverser);
    }

    public static final void start(Step<?, ?> step, Traverser.Admin<?> traverser) {
        traverser.getSideEffects().<GlobalMetrics>get(ProfileStep.METRICS_KEY).startInternal(step);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Global Metrics: \n");
        timers.forEach((label, timer) -> sb.append(timer).append("\n"));
        sb.append("\n");
        return sb.toString();
    }

    public static GlobalMetrics merge(Iterator<GlobalMetrics> metrics) {
        GlobalMetrics total = new GlobalMetrics();
        metrics.forEachRemaining(m -> total.aggregate(m));
        return total;
    }

    private void aggregate(GlobalMetrics m) {
        m.timers.forEach((label, timer) -> {
            StepMetrics stepMetrics = this.timers.get(label);
            if (null == stepMetrics) {
                stepMetrics = new StepMetrics(timer);
                this.timers.put(label, stepMetrics);
            }
            stepMetrics.aggregate(timer);
        });
    }
}
