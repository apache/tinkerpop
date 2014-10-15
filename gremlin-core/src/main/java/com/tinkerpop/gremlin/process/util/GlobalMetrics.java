package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class GlobalMetrics implements Serializable {
    private static final String[] headers = {"Step", "Count", "Time (ms)", "Time (ns)", "% Duration"};

    private long totalStepDuration;

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
        // TODO profile
        //        if (totalStepDuration == 0) {
        //            return "Metrics not enabled.";
        //        }

        // Build a pretty table of metrics data.

        // Append headers
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%30s%15s%15s%15s%15s", headers));

        // Append each StepMetric's row
        for (StepMetrics s : this.stepMetrics.values()) {
            sb.append(String.format("%n%30s%15d%15f%15d%15f",
                    s.getName(), s.getCounts(), s.getTimeMs(), s.getTimeNs(), s.getPercentageDuration()));
        }

        // Append total duration
        sb.append(String.format("%n%30s%15s%15f%15s%15s",
                "TOTAL", "-", getTotalStepDurationMs(), "-", "-"));

        return sb.toString();
    }

    public double getTotalStepDurationMs() {
        return totalStepDuration / 1000000.0d;
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
