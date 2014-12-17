package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;

import java.io.Serializable;
import java.util.*;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class TraversalMetricsUtil implements TraversalMetrics, Serializable {
    private static final String[] HEADERS = {"Step", "Count", "Traversers", "Time (ms)", "% Dur"};

    private long totalStepDuration;

    private final Map<String, StepCounter> stepTimers = new LinkedHashMap<>();
    private final LinkedList<StepCounter> orderedStepCounters = new LinkedList<>();

    public TraversalMetricsUtil() {
    }

    public void start(final ProfileStep<?> step) {
        this.stepTimers.get(step.getLabel()).start();
    }

    public void stop(final ProfileStep<?> step) {
        this.stepTimers.get(step.getLabel()).stop();
    }

    public void finish(final ProfileStep<?> step, final Traverser.Admin<?> traverser) {
        this.stepTimers.get(step.getLabel()).finish(traverser);
    }

    @Override
    public String toString() {
        computeTotals();

        // Build a pretty table of metrics data.

        // Append headers
        StringBuilder sb = new StringBuilder();
        sb.append("Traversal Metrics\n").append(String.format("%28s %13s %11s %15s %8s", HEADERS));

        // Append each StepMetric's row. These are in reverse order.
        for (StepCounter s : this.orderedStepCounters) {
            String rowName = s.getName();

            if (rowName.length() > 28)
                rowName = rowName.substring(0, 28 - 3) + "...";

            sb.append(String.format("%n%28s %13d %11d %15.3f %8.2f",
                    rowName, s.getCount(), s.getTraversers(), s.getTimeMs(), s.getPercentageDuration()));
        }

        // Append total duration
        sb.append(String.format("%n%28s %13s %11s %15.3f %8s",
                "TOTAL", "-", "-", getDurationMs(), "-"));

        return sb.toString();
    }

    private void computeTotals() {
        // Set upstream StepCounter so the upstream time can be deducted from the downstream total
        StepCounter prev = null;
        for (StepCounter stepTimer : orderedStepCounters) {
            if (prev != null) {
                stepTimer.setPreviousStepCounter(prev);
            }
            prev = stepTimer;
        }

        // Calculate total duration of all steps
        this.totalStepDuration = 0;
        final Collection<StepCounter> timers = this.stepTimers.values();
        timers.forEach(step ->
                        this.totalStepDuration += step.getTimeNs()
        );

        // Assign step %'s
        timers.forEach(step ->
                        step.setPercentageDuration(step.getTimeNs() * 100.d / this.totalStepDuration)
        );
    }

    public double getDurationMs() {
        return this.totalStepDuration / 1000000.0d;
    }

    public static TraversalMetricsUtil merge(final Iterator<TraversalMetricsUtil> metrics) {
        final TraversalMetricsUtil traversalMetricsUtil = new TraversalMetricsUtil();
        metrics.forEachRemaining(globalMetrics -> {
            globalMetrics.stepTimers.forEach((label, stepCounter) -> {
                StepCounter stepMetrics = traversalMetricsUtil.stepTimers.get(label);
                if (null == stepMetrics) {
                    if (stepCounter instanceof StepTimer) {
                        stepMetrics = new StepTimer((StepTimer) stepCounter);
                    } else {
                        stepMetrics = new StepCounter(stepCounter);
                    }
                    traversalMetricsUtil.stepTimers.put(label, stepMetrics);
                    traversalMetricsUtil.orderedStepCounters.add(stepMetrics);
                }
                stepMetrics.aggregate(stepCounter);
            });
        });
        return traversalMetricsUtil;
    }

    public StepMetrics getStepMetrics(final int index) {
        return orderedStepCounters.get(index);
    }

    public StepMetrics getStepMetrics(final String stepLabel) {
        return stepTimers.get(stepLabel);
    }

    public void initialize(final ProfileStep step, final boolean timingEnabled) {
        if (stepTimers.containsKey(step.getLabel())) {
            return;
        }

        StepCounter stepMetrics = null;
        if (timingEnabled) {
            stepMetrics = new StepTimer(step);
        } else {
            stepMetrics = new StepCounter(step);
        }
        this.stepTimers.put(step.getLabel(), stepMetrics);
        this.orderedStepCounters.push(stepMetrics);
    }
}
