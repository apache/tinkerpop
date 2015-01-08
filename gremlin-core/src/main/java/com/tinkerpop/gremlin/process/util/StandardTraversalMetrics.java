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
    private final Map<String, MutableMetrics> metrics = new HashMap<>();
    private final Map<Integer, String> indexToLabelMap = new HashMap<>();

    /*
    The following are computed values upon the completion of profiling in order to report the results back to the user
     */
    private long totalStepDuration;
    private Map<String, ImmutableMetrics> computedMetrics;

    public StandardTraversalMetrics() {
    }

    public void start(final String metricsId) {
        dirty = true;
        metrics.get(metricsId).start();
    }

    public void stop(final String metricsId) {
        dirty = true;
        metrics.get(metricsId).stop();
    }

    public void finish(final String metricsId, final long bulk) {
        dirty = true;
        final MutableMetrics m = metrics.get(metricsId);
        m.finish(1);
        m.getNested(ELEMENT_COUNT_ID).incrementCount(bulk);
    }


    @Override
    public long getDuration(final TimeUnit unit) {
        computeTotals();
        return unit.convert(totalStepDuration, MutableMetrics.SOURCE_UNIT);
    }

    @Override
    public Metrics getMetrics(final int index) {
        computeTotals();
        // adjust index to account for the injected profile steps
        return (Metrics) computedMetrics.get(indexToLabelMap.get(index * 2 + 1));
    }

    @Override
    public Metrics getMetrics(final String stepLabel) {
        computeTotals();
        return computedMetrics.get(stepLabel);
    }

    @Override
    public Collection<ImmutableMetrics> getMetrics() {
        computeTotals();
        return computedMetrics.values();
    }

    @Override
    public String toString() {
        computeTotals();

        // Build a pretty table of metrics data.

        // Append headers
        StringBuilder sb = new StringBuilder();
        sb.append("Traversal Metrics\n").append(String.format("%28s %13s %11s %15s %8s", HEADERS));

        // Append each StepMetric's row.
        for (ImmutableMetrics s : computedMetrics.values()) {
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

        List<MutableMetrics> tempMetrics = new ArrayList<>(metrics.size());
        metrics.values().forEach(m -> tempMetrics.add(m.clone()));

        // Subtract upstream traversal time from each step
        for (int ii = tempMetrics.size() - 1; ii > 0; ii--) {
            MutableMetrics cur = tempMetrics.get(ii);
            MutableMetrics upStream = tempMetrics.get(ii - 1);
            cur.setDuration(cur.getDuration(MutableMetrics.SOURCE_UNIT) - upStream.getDuration(MutableMetrics.SOURCE_UNIT));
        }

        // Calculate total duration
        this.totalStepDuration = 0;
        tempMetrics.forEach(m -> this.totalStepDuration += m.getDuration(MutableMetrics.SOURCE_UNIT));

        // Assign %'s
        tempMetrics.forEach(m ->
                        m.setPercentDuration(m.getDuration(TimeUnit.NANOSECONDS) * 100.d / this.totalStepDuration)
        );

        // Store immutable instances of the calculated metrics
        computedMetrics = new HashMap<>(metrics.size());
        tempMetrics.forEach(it -> computedMetrics.put(it.getId(), it.getImmutableClone()));

        dirty = false;
    }

    public static StandardTraversalMetrics merge(final Iterator<StandardTraversalMetrics> toMerge) {
        final StandardTraversalMetrics newMetrics = new StandardTraversalMetrics();

        // iterate the incoming TraversalMetrics
        toMerge.forEachRemaining(inTraversalMetrics -> {

            // aggregate the internal Metrics
            inTraversalMetrics.metrics.forEach((metricsId, toAggregate) -> {

                MutableMetrics aggregateMetrics = newMetrics.metrics.get(metricsId);
                if (null == aggregateMetrics) {
                    // need to create a Metrics to aggregate into
                    aggregateMetrics = new MutableMetrics(toAggregate.getId(), toAggregate.getName());

                    newMetrics.metrics.put(metricsId, aggregateMetrics);
                    // Set the index of the Metrics
                    for (Map.Entry<Integer, String> entry : newMetrics.indexToLabelMap.entrySet()) {
                        if (metricsId.equals(entry.getValue())) {
                            newMetrics.indexToLabelMap.put(entry.getKey(), metricsId);
                            break;
                        }
                    }
                }
                aggregateMetrics.aggregate(toAggregate);
            });
        });
        return newMetrics;
    }

    public void initializeIfNecessary(final String metricsId, final int index, final String displayName) {
        if (indexToLabelMap.containsKey(index)) {
            return;
        }

        MutableMetrics newMetrics = new MutableMetrics(metricsId, displayName);
        // Add a nested metric for item count
        newMetrics.addNested(new MutableMetrics(ELEMENT_COUNT_ID, ITEM_COUNT_DISPLAY));

        // The index is necessary to ensure that step order is preserved after a merge.
        indexToLabelMap.put(index, metricsId);
        metrics.put(metricsId, newMetrics);
    }
}
