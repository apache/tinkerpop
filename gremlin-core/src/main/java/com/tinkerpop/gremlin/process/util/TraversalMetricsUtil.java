package com.tinkerpop.gremlin.process.util;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class TraversalMetricsUtil implements TraversalMetrics, Serializable {
    // toString() specific headers
    private static final String[] HEADERS = {"Step", "Count", "Traversers", "Time (ms)", "% Dur"};

    private static final String ITEM_COUNT_DISPLAY = "item count";

    private long totalStepDuration;

    private final Map<String, MetricsUtil> metrics = new LinkedHashMap<>();
    private final Map<Integer, MetricsUtil> orderedMetrics = new TreeMap<>();

    public TraversalMetricsUtil() {
    }

    public void start(final String metricsId) {
        this.metrics.get(metricsId).start();
    }

    public void stop(final String metricsId) {
        this.metrics.get(metricsId).stop();
    }

    public void finish(final String metricsId, final long bulk) {
        final MetricsUtil metricsUtil = this.metrics.get(metricsId);
        metricsUtil.finish(1);
        metricsUtil.getNested(ELEMENT_COUNT_ID).incrementCount(bulk);
    }

    @Override
    public String toString() {
        List<MetricsUtil> snapshot = computeTotals();

        // Build a pretty table of metrics data.

        // Append headers
        StringBuilder sb = new StringBuilder();
        sb.append("Traversal Metrics\n").append(String.format("%28s %13s %11s %15s %8s", HEADERS));

        // Append each StepMetric's row.
        for (MetricsUtil s : snapshot) {
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

    private List<MetricsUtil> computeTotals() {
        // Create a temporary copy of all the Metrics
        List<MetricsUtil> copy = new ArrayList<>(orderedMetrics.size());
        orderedMetrics.values().forEach(metrics -> copy.add(metrics.clone()));

        // Subtract upstream traversal time from each step
        for (int ii = copy.size() - 1; ii > 0; ii--) {
            MetricsUtil cur = copy.get(ii);
            MetricsUtil upStream = copy.get(ii - 1);
            cur.setDuration(cur.getDuration(MetricsUtil.SOURCE_UNIT) - upStream.getDuration(MetricsUtil.SOURCE_UNIT));
        }

        // Calculate total duration
        this.totalStepDuration = 0;
        copy.forEach(metrics -> this.totalStepDuration += metrics.getDuration(MetricsUtil.SOURCE_UNIT));

        // Assign %'s
        copy.forEach(metrics ->
                        metrics.setPercentDuration(metrics.getDuration(TimeUnit.NANOSECONDS) * 100.d / this.totalStepDuration)
        );

        return copy;
    }

    public static TraversalMetricsUtil merge(final Iterator<TraversalMetricsUtil> toMerge) {
        final TraversalMetricsUtil traversalMetricsUtil = new TraversalMetricsUtil();
        toMerge.forEachRemaining(incomingMetrics -> {
            incomingMetrics.orderedMetrics.forEach((index, toAggregate) -> {
                MetricsUtil aggregateMetrics = traversalMetricsUtil.metrics.get(toAggregate.getId());
                if (null == aggregateMetrics) {
                    aggregateMetrics = new MetricsUtil(toAggregate.getId(), toAggregate.getName());
                    traversalMetricsUtil.metrics.put(aggregateMetrics.getId(), aggregateMetrics);
                    traversalMetricsUtil.orderedMetrics.put(index, aggregateMetrics);
                }
                aggregateMetrics.aggregate(toAggregate);
            });
        });
        return traversalMetricsUtil;
    }

    @Override
    public long getDuration(final TimeUnit unit) {
        return unit.convert(totalStepDuration, MetricsUtil.SOURCE_UNIT);
    }

    @Override
    public Metrics getMetrics(final int index) {
        return (Metrics) orderedMetrics.values().toArray()[index];
    }

    @Override
    public Metrics getMetrics(final String stepLabel) {
        return metrics.get(stepLabel);
    }

    // The index is necessary to ensure that step order is preserved after a merge.
    public void initializeIfNecessary(final String metricsId, final int index, final String displayName) {
        if (metrics.containsKey(metricsId)) {
            return;
        }

        MetricsUtil metrics = new MetricsUtil(metricsId, displayName);
        // Add a nested metric for item count
        metrics.addNested(new MetricsUtil(ELEMENT_COUNT_ID, ITEM_COUNT_DISPLAY));

        this.metrics.put(metricsId, metrics);
        this.orderedMetrics.put(index, metrics);
    }
}
