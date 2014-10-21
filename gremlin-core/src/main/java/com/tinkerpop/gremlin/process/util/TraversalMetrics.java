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
    private static final String[] headers = {"Step", "Count", "Traversers", "Time (ms)", "% Dur"};
    private static final WeakHashMap<Traversal, Boolean> PROFILING_CACHE = new WeakHashMap<Traversal, Boolean>();

    private long totalStepDuration;

    private final Map<String, StepTimer> stepTimers = new LinkedHashMap<String, StepTimer>();

    private TraversalMetrics() {

    }

    public static final void start(final Step<?, ?> step) {
        if (!profiling(step.getTraversal())) {
            return;
        }

        step.getTraversal().sideEffects().getOrCreate(ProfileStep.METRICS_KEY, TraversalMetrics::new).startInternal(step);
    }

    public static final void stop(final Step<?, ?> step) {
        if (!profiling(step.getTraversal())) {
            return;
        }

        step.getTraversal().sideEffects().<TraversalMetrics>get(ProfileStep.METRICS_KEY).stopInternal(step);
    }

    public static final void finish(final Step<?, ?> step, Traverser.Admin<?> traverser) {
        if (!profiling(step.getTraversal())) {
            return;
        }

        step.getTraversal().sideEffects().<TraversalMetrics>get(ProfileStep.METRICS_KEY).finishInternal(step, traverser);
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
        sb.append("Traversal Metrics\n").append(String.format("%32s%12s%11s%16s%8s", headers));

        // Append each StepMetric's row
        for (StepTimer s : this.stepTimers.values()) {
            sb.append(String.format("%n%32s%12d%11d%16.3f%8.2f",
                    s.getName(), s.getCount(), s.getTraversers(), s.getTimeMs(), s.getPercentageDuration()));
        }

        // Append total duration
        sb.append(String.format("%n%32s%12s%11s%16.3f%8s",
                "TOTAL", "-", "-", getTotalStepDurationMs(), "-"));

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

    public StepMetrics getStepMetrics(final String stepLabel) {
        return this.stepTimers.get(stepLabel);
    }

    public Set<String> getStepLabels() {
        return this.stepTimers.keySet();
    }
}
