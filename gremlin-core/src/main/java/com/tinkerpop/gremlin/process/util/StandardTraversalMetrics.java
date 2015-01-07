package com.tinkerpop.gremlin.process.util;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class StandardTraversalMetrics implements TraversalMetrics, Serializable {
    // toString() specific headers
    private static final String[] HEADERS = {"Step", "Count", "Traversers", "Time (ms)", "% Dur"};

    private static final String ITEM_COUNT_DISPLAY = "item count";

    private boolean dirty = true;
    private final Map<String, MutableMetrics> metrics = new LinkedHashMap<>();
    private final Map<Integer, MutableMetrics> orderedMetrics = new TreeMap<>();

    /*
    The following are computed values upon the completion of profiling in order to report the results back to the user
     */
    private long totalStepDuration;
    private List<ImmutableMetrics> computedMetrics;

    public StandardTraversalMetrics() {
    }

    public void start(final String metricsId) {
        dirty = true;
        this.metrics.get(metricsId).start();
    }

    public void stop(final String metricsId) {
        dirty = true;
        this.metrics.get(metricsId).stop();
    }

    public void finish(final String metricsId, final long bulk) {
        dirty = true;
        final MutableMetrics metricsUtil = this.metrics.get(metricsId);
        metricsUtil.finish(1);
        metricsUtil.getNested(ELEMENT_COUNT_ID).incrementCount(bulk);
    }


    @Override
    public long getDuration(final TimeUnit unit) {
        computeTotals();
        return unit.convert(totalStepDuration, MutableMetrics.SOURCE_UNIT);
    }

    @Override
    public Metrics getMetrics(final int index) {
        computeTotals();
        return (Metrics) orderedMetrics.values().toArray()[index];
    }

    @Override
    public Metrics getMetrics(final String stepLabel) {
        computeTotals();
        return metrics.get(stepLabel);
    }

    @Override
    public Collection<ImmutableMetrics> getMetrics() {
        computeTotals();
        return computedMetrics;
    }

    @Override
    public String toString() {
        computeTotals();

        // Build a pretty table of metrics data.

        // Append headers
        StringBuilder sb = new StringBuilder();
        sb.append("Traversal Metrics\n").append(String.format("%28s %13s %11s %15s %8s", HEADERS));

        // Append each StepMetric's row.
        for (ImmutableMetrics s : computedMetrics) {
            String rowName = s.getName();

            if (rowName.length() > 28)
                rowName = rowName.substring(0, 28 - 3) + "...";

            long itemCount = s.getNested(ELEMENT_COUNT_ID).getCount();

            sb.append(String.format("%n%28s %13d %11d %15.3f %8.2f",
                    rowName, itemCount, s.getCount(), s.getDuration(TimeUnit.MICROSECONDS) / 1000.0, s.getPercentDuration()));
        }

        // Append total duration
        sb.append(String.format("%n%28s %13s %11s %15.3f %8s",
                "TOTAL", "-", "-", getDuration(TimeUnit.MICROSECONDS) / 1000.0, "-"));

        return sb.toString();
    }

    private void computeTotals() {
        if (!dirty) {
            // already good to go
            return;
        }

        List<MutableMetrics> tempMetrics = new ArrayList<>(orderedMetrics.size());
        orderedMetrics.values().forEach(metrics -> tempMetrics.add(metrics.clone()));

        // Subtract upstream traversal time from each step
        for (int ii = tempMetrics.size() - 1; ii > 0; ii--) {
            MutableMetrics cur = tempMetrics.get(ii);
            MutableMetrics upStream = tempMetrics.get(ii - 1);
            cur.setDuration(cur.getDuration(MutableMetrics.SOURCE_UNIT) - upStream.getDuration(MutableMetrics.SOURCE_UNIT));
        }

        // Calculate total duration
        this.totalStepDuration = 0;
        tempMetrics.forEach(metrics -> this.totalStepDuration += metrics.getDuration(MutableMetrics.SOURCE_UNIT));

        // Assign %'s
        tempMetrics.forEach(metrics ->
                        metrics.setPercentDuration(metrics.getDuration(TimeUnit.NANOSECONDS) * 100.d / this.totalStepDuration)
        );

        // Store immutable instances of the calculated metrics
        computedMetrics = new ArrayList<>(orderedMetrics.size());
        tempMetrics.forEach(it -> computedMetrics.add(it.getImmutableClone()));

        dirty = false;
    }

    public static StandardTraversalMetrics merge(final Iterator<StandardTraversalMetrics> toMerge) {
        final StandardTraversalMetrics traversalMetricsUtil = new StandardTraversalMetrics();
        toMerge.forEachRemaining(incomingMetrics -> {
            incomingMetrics.orderedMetrics.forEach((index, toAggregate) -> {
                MutableMetrics aggregateMetrics = traversalMetricsUtil.metrics.get(toAggregate.getId());
                if (null == aggregateMetrics) {
                    aggregateMetrics = new MutableMetrics(toAggregate.getId(), toAggregate.getName());
                    traversalMetricsUtil.metrics.put(aggregateMetrics.getId(), aggregateMetrics);
                    traversalMetricsUtil.orderedMetrics.put(index, aggregateMetrics);
                }
                aggregateMetrics.aggregate(toAggregate);
            });
        });
        return traversalMetricsUtil;
    }

    // The index is necessary to ensure that step order is preserved after a merge.
    public void initializeIfNecessary(final String metricsId, final int index, final String displayName) {
        if (metrics.containsKey(metricsId)) {
            return;
        }

        MutableMetrics metrics = new MutableMetrics(metricsId, displayName);
        // Add a nested metric for item count
        metrics.addNested(new MutableMetrics(ELEMENT_COUNT_ID, ITEM_COUNT_DISPLAY));

        this.metrics.put(metricsId, metrics);
        this.orderedMetrics.put(index, metrics);
    }

}
