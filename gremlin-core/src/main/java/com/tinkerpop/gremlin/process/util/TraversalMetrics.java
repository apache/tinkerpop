package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class TraversalMetrics implements Serializable {
    public static final String PROFILING_ENABLED = "tinkerpop.profiling";
    private static final String[] HEADERS = {"Step", "Count", "Traversers", "Time (ms)", "% Dur"};
    private static final WeakHashMap<Traversal, Boolean> PROFILING_CACHE = new WeakHashMap<>();

    private long totalStepDuration;

    private final Map<String, StepTimer> stepTimers = new LinkedHashMap<String, StepTimer>();

    private TraversalMetrics() {

    }

    public static final void start(final Step<?, ?> step) {
        if (!profiling(step.getTraversal())) {
            return;
        }

        step.getTraversal().asAdmin().getSideEffects().getOrCreate(ProfileStep.METRICS_KEY, TraversalMetrics::new).startInternal(step);
    }

    public static final void stop(final Step<?, ?> step) {
        if (!profiling(step.getTraversal())) {
            return;
        }

        step.getTraversal().asAdmin().getSideEffects().<TraversalMetrics>get(ProfileStep.METRICS_KEY).stopInternal(step);
    }

    public static final void finish(final Step<?, ?> step, final Traverser.Admin<?> traverser) {
        if (!profiling(step.getTraversal())) {
            return;
        }

        step.getTraversal().asAdmin().getSideEffects().<TraversalMetrics>get(ProfileStep.METRICS_KEY).finishInternal(step, traverser);
    }

    private static boolean profiling(final Traversal<?, ?> traversal) {
        Boolean profiling;
        if ((profiling = PROFILING_CACHE.get(traversal)) != null)
            return profiling;
        profiling = TraversalHelper.hasStepOfClass(ProfileStep.class, traversal);
        PROFILING_CACHE.put(traversal, profiling);
        return profiling;
    }

    private void startInternal(final Step<?, ?> step) {
        StepTimer stepMetrics = this.stepTimers.get(step.getLabel().orElse(step.getId()));
        if (null == stepMetrics) {
            stepMetrics = new StepTimer(step);
            this.stepTimers.put(step.getLabel().orElse(step.getId()), stepMetrics);
        }
        stepMetrics.start();
    }

    private void stopInternal(final Step<?, ?> step) {
        this.stepTimers.get(step.getLabel().orElse(step.getId())).stop();
    }

    private void finishInternal(final Step<?, ?> step, final Traverser.Admin<?> traverser) {
        this.stepTimers.get(step.getLabel().orElse(step.getId())).finish(traverser);
    }

    @Override
    public String toString() {
        computeTotals();

        // Build a pretty table of metrics data.

        // Append headers
        StringBuilder sb = new StringBuilder();
        sb.append("Traversal Metrics\n").append(String.format("%28s %13s %11s %15s %8s", HEADERS));

        // Append each StepMetric's row
        for (StepTimer s : this.stepTimers.values()) {
            sb.append(String.format("%n%28s %13d %11d %15.3f %8.2f",
                    s.getShortName(28), s.getCount(), s.getTraversers(), s.getTimeMs(), s.getPercentageDuration()));
        }

        // Append total duration
        sb.append(String.format("%n%28s %13s %11s %15.3f %8s",
                "TOTAL", "-", "-", getTotalStepDurationMs(), "-"));

        return sb.toString();
    }

    private void computeTotals() {
        // Calculate total duration of all steps (note that this does not include non-step Traversal framework time
        this.totalStepDuration = 0;
        this.stepTimers.values().forEach(step -> {
            this.totalStepDuration += step.getTimeNs();
        });

        // Assign step %'s
        this.stepTimers.values().forEach(step -> {
            step.setPercentageDuration(step.getTimeNs() * 100.d / this.totalStepDuration);
        });
    }

    public double getTotalStepDurationMs() {
        return this.totalStepDuration / 1000000.0d;
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

    public StepMetrics getStepMetrics(final String stepLabel) {
        return this.stepTimers.get(stepLabel);
    }

    public Set<String> getStepLabels() {
        return this.stepTimers.keySet();
    }
}
