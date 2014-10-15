package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class GlobalMetrics implements Serializable {

    private final Map<String, StepMetrics> stepMetrics = new LinkedHashMap<>();

    public static final void start(final Step<?, ?> step, final Traverser.Admin<?> traverser) {
        traverser.getSideEffects().getOrCreate(ProfileStep.METRICS_KEY, GlobalMetrics::new).startInternal(step);
    }

    public static final void stop(final Step<?, ?> step, Traverser.Admin<?> traverser) {
        traverser.getSideEffects().<GlobalMetrics>get(ProfileStep.METRICS_KEY).stopInternal(step);
    }

    public static final void finish(final Step<?, ?> step, Traverser.Admin<?> traverser) {
        traverser.getSideEffects().<GlobalMetrics>get(ProfileStep.METRICS_KEY).finishInternal(step, traverser);
    }

    private void startInternal(final Step<?, ?> step) {
        StepMetrics stepMetrics = this.stepMetrics.get(step.getLabel());
        if (null == stepMetrics) {
            stepMetrics = new StepMetrics(step);
            this.stepMetrics.put(step.getLabel(), stepMetrics);
        }
        stepMetrics.startTimer();
    }

    private void stopInternal(final Step<?, ?> step) {
        this.stepMetrics.get(step.getLabel()).stop();
    }

    private void finishInternal(final Step<?, ?> step, final Traverser.Admin<?> traverser) {
        this.stepMetrics.get(step.getLabel()).finish(traverser);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Global Metrics: \n");
        stepMetrics.forEach((label, timer) -> sb.append(timer).append("\n"));
        sb.append("\n");
        return sb.toString();
    }

    public static GlobalMetrics merge(final Iterator<GlobalMetrics> metrics) {
        final GlobalMetrics totalMetrics = new GlobalMetrics();
        metrics.forEachRemaining(globalMetrics -> {
            globalMetrics.stepMetrics.forEach((label, timer) -> {
                StepMetrics stepMetrics = totalMetrics.stepMetrics.get(label);
                if (null == stepMetrics) {
                    stepMetrics = new StepMetrics(timer);
                    totalMetrics.stepMetrics.put(label, stepMetrics);
                }
                stepMetrics.aggregate(timer);
            });
        });
        return totalMetrics;
    }
}
