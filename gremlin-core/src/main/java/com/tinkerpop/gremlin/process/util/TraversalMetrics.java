package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TraversalMetrics implements Serializable {
    private static final String[] headers = {"Step", "Count", "Traversers", "Time (ms)", "Time (ns)", "% Duration"};

    private long totalStepDuration;

    private final Map<String, StepTimer> stepTimers = new LinkedHashMap<>();

    public static final void start(final Step<?, ?> step, final Traverser.Admin<?> traverser) {
        traverser.getSideEffects().getOrCreate(ProfileStep.METRICS_KEY, TraversalMetrics::new).startInternal(step);
    }

    public static final void stop(final Step<?, ?> step, Traverser.Admin<?> traverser) {
        traverser.getSideEffects().<TraversalMetrics>get(ProfileStep.METRICS_KEY).stopInternal(step);
    }

    public static final void finish(final Step<?, ?> step, Traverser.Admin<?> traverser) {
        traverser.getSideEffects().<TraversalMetrics>get(ProfileStep.METRICS_KEY).finishInternal(step, traverser);
    }

    private void startInternal(final Step<?, ?> step) {
        StepTimer stepMetrics = this.stepTimers.get(step.getLabel());
        if (null == stepMetrics) {
            stepMetrics = new StepTimer(step);
            this.stepTimers.put(step.getLabel(), stepMetrics);
        }
        stepMetrics.start();
    }

    private void stopInternal(final Step<?, ?> step) {
        this.stepTimers.get(step.getLabel()).stop();
    }

    private void finishInternal(final Step<?, ?> step, final Traverser.Admin<?> traverser) {
        this.stepTimers.get(step.getLabel()).finish(traverser);
    }

    @Override
    public String toString() {
        computeTotals();

        // Build a pretty table of metrics data.

        // Append headers
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%30s%15s%15s%15s%15s%15s", headers));

        // Append each StepMetric's row
        for (StepTimer s : this.stepTimers.values()) {
            sb.append(String.format("%n%30s%15d%15d%15f%15d%15f",
                    s.getName(), s.getCount(), s.getTraversers(), s.getTimeMs(), s.getTimeNs(), s.getPercentageDuration()));
        }

        // Append total duration
        sb.append(String.format("%n%30s%15s%15s%15f%15s%15s",
                "TOTAL", "-", "-", getTotalStepDurationMs(), "-", "-"));

        return sb.toString();
    }

    private void computeTotals() {
        // Calculate total duration of all steps (note that this does not include non-step Traversal framework time
        totalStepDuration = 0;
        this.stepTimers.values().forEach(step -> {
            totalStepDuration += step.getTimeNs();
        });

        // Assign step %'s
        this.stepTimers.values().forEach(step -> {
            step.setPercentageDuration(step.getTimeNs() * 100.d / totalStepDuration);
        });
    }

    public double getTotalStepDurationMs() {
        return totalStepDuration / 1000000.0d;
    }

    public static TraversalMetrics merge(final Iterator<TraversalMetrics> metrics) {
        final TraversalMetrics totalMetrics = new TraversalMetrics();
        metrics.forEachRemaining(globalMetrics -> {
            globalMetrics.stepTimers.forEach((label, timer) -> {
                StepTimer stepMetrics = totalMetrics.stepTimers.get(label);
                if (null == stepMetrics) {
                    stepMetrics = new StepTimer(timer.getName(), timer.getLabel());
                    totalMetrics.stepTimers.put(label, stepMetrics);
                }
                stepMetrics.aggregate(timer);
            });
        });
        return totalMetrics;
    }
}
