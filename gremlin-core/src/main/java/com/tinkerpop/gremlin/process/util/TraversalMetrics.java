package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;

import java.io.Serializable;
import java.util.*;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class TraversalMetrics implements Serializable {
    public static final String PROFILING_ENABLED = "tinkerpop.profiling";
    private static final String[] HEADERS = {"Step", "Count", "Traversers", "Time (ms)", "% Dur"};
    private static final WeakHashMap<Traversal, Boolean> PROFILING_CACHE = new WeakHashMap<Traversal, Boolean>();

    private long totalStepDuration;

    private final Map<String, StepTimer> stepTimers = new LinkedHashMap<String, StepTimer>();
    private final LinkedList<StepTimer> orderedStepTimers = new LinkedList<StepTimer>();

    public TraversalMetrics() {

    }

    public void start(final ProfileStep<?> step) {
        StepTimer stepMetrics = this.stepTimers.get(step.getLabel());
        if (null == stepMetrics) {
            stepMetrics = new StepTimer(step);
            this.stepTimers.put(step.getLabel(), stepMetrics);
            this.orderedStepTimers.push(stepMetrics);
        }
        stepMetrics.start();
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
        for (StepTimer s : this.orderedStepTimers) {
            String rowName = s.getName();

            if (rowName.length() > 28)
                rowName = rowName.substring(0, 28 - 3) + "...";

            sb.append(String.format("%n%28s %13d %11d %15.3f %8.2f",
                    rowName, s.getCount(), s.getTraversers(), s.getTimeMs(), s.getPercentageDuration()));
        }

        // Append total duration
        sb.append(String.format("%n%28s %13s %11s %15.3f %8s",
                "TOTAL", "-", "-", getTotalStepDurationMs(), "-"));

        return sb.toString();
    }

    private void computeTotals() {

        // Set upstream StepTimer so the upstream time can be deducted from the downstream total
        StepTimer prev = null;
        for (StepTimer stepTimer : orderedStepTimers) {
            if (prev != null) {
                stepTimer.setPreviousStepTimer(prev);
            }
            prev = stepTimer;
        }

        // Calculate total duration of all steps
        this.totalStepDuration = 0;
        final Collection<StepTimer> timers = this.stepTimers.values();
        timers.forEach(step ->
            this.totalStepDuration += step.getTimeNs()
        );

        // Assign step %'s
        timers.forEach(step ->
            step.setPercentageDuration(step.getTimeNs() * 100.d / this.totalStepDuration)
        );
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

    public StepMetrics getStepMetrics(final int index){
        return orderedStepTimers.get(index);
    }
}
