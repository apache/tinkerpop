/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class StandardTraversalMetrics implements TraversalMetrics, Serializable {
    // toString() specific headers
    private static final String[] HEADERS = {"Step", "Count", "Traversers", "Time (ms)", "% Dur"};

    private boolean dirty = true;
    private final Map<String, MutableMetrics> metrics = new HashMap<>();
    private final Map<String, MutableMetrics> allMetrics = new HashMap<>();
    private final TreeMap<Integer, String> indexToLabelMap = new TreeMap<>();

    /*
    The following are computed values upon the completion of profiling in order to report the results back to the user
     */
    private long totalStepDuration;
    private Map<String, ImmutableMetrics> computedMetrics;

    public StandardTraversalMetrics() {
    }

    public void start(final String metricsId) {
        dirty = true;
        allMetrics.get(metricsId).start();
    }

    public void stop(final String metricsId) {
        dirty = true;
        allMetrics.get(metricsId).stop();
    }

    public void finish(final String metricsId, final long bulk) {
        dirty = true;
        final MutableMetrics metrics = allMetrics.get(metricsId);
        metrics.stop();
        metrics.incrementCount(TRAVERSER_COUNT_ID, 1);
        metrics.incrementCount(ELEMENT_COUNT_ID, bulk);
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
        return (Metrics) computedMetrics.get(indexToLabelMap.get(index));
    }

    @Override
    public Metrics getMetrics(final String id) {
        computeTotals();
        return computedMetrics.get(id);
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
        final StringBuilder sb = new StringBuilder("Traversal Metrics\n")
                .append(String.format("%-50s %21s %11s %15s %8s", HEADERS));

        sb.append("\n=============================================================================================================");

        appendMetrics(computedMetrics.values(), sb, 0);

        // Append total duration
        sb.append(String.format("%n%50s %21s %11s %15.3f %8s",
                ">TOTAL", "-", "-", getDuration(TimeUnit.MICROSECONDS) / 1000.0, "-"));

        return sb.toString();
    }

    private void appendMetrics(final Collection<? extends Metrics> metrics, final StringBuilder sb, final int indent) {
        // Append each StepMetric's row. indexToLabelMap values are ordered by index.
        for (Metrics m : metrics) {
            String rowName = m.getName();

            // Handle indentation
            for (int ii = 0; ii < indent; ii++) {
                rowName = "  " + rowName;
            }
            // Abbreviate if necessary
            rowName = StringUtils.abbreviate(rowName, 50);

            // Grab the values
            final Long itemCount = m.getCount(ELEMENT_COUNT_ID);
            final Long traverserCount = m.getCount(TRAVERSER_COUNT_ID);
            Double percentDur = (Double) m.getAnnotation(PERCENT_DURATION_KEY);

            // Build the row string

            sb.append(String.format("%n%-50s", rowName));

            if (itemCount != null) {
                sb.append(String.format(" %21d", itemCount));
            } else {
                sb.append(String.format(" %21s", ""));
            }

            if (traverserCount != null) {
                sb.append(String.format(" %11d", traverserCount));
            } else {
                sb.append(String.format(" %11s", ""));
            }

            sb.append(String.format(" %15.3f", m.getDuration(TimeUnit.MICROSECONDS) / 1000.0));

            if (percentDur!=null){
                sb.append(String.format(" %8.2f", percentDur ));
            }

            appendMetrics(m.getNested(), sb, indent + 1);
        }
    }

    private void computeTotals() {
        if (!dirty) {
            // already good to go
            return;
        }

        // Create temp list of ordered metrics
        List<MutableMetrics> tempMetrics = new ArrayList<>(metrics.size());
        for (String label : indexToLabelMap.values()) {
            // The indexToLabelMap is sorted by index (key)
            tempMetrics.add(metrics.get(label).clone());
        }

        // Calculate total duration
        this.totalStepDuration = 0;
        tempMetrics.forEach(m -> this.totalStepDuration += m.getDuration(MutableMetrics.SOURCE_UNIT));

        // Assign %'s
        tempMetrics.forEach(m -> {
            double dur = m.getDuration(TimeUnit.NANOSECONDS) * 100.d / this.totalStepDuration;
            m.setAnnotation(PERCENT_DURATION_KEY, dur);
        });

        // Store immutable instances of the calculated metrics
        computedMetrics = new LinkedHashMap<>(metrics.size());
        tempMetrics.forEach(it -> computedMetrics.put(it.getId(), it.getImmutableClone()));

        dirty = false;
    }

    public static StandardTraversalMetrics merge(final Iterator<StandardTraversalMetrics> toMerge) {
        final StandardTraversalMetrics newTraversalMetrics = new StandardTraversalMetrics();

        // iterate the incoming TraversalMetrics
        toMerge.forEachRemaining(inTraversalMetrics -> {
            // aggregate the internal Metrics
            inTraversalMetrics.metrics.forEach((metricsId, toAggregate) -> {

                MutableMetrics aggregateMetrics = newTraversalMetrics.metrics.get(metricsId);
                if (null == aggregateMetrics) {
                    // need to create a Metrics to aggregate into
                    aggregateMetrics = new MutableMetrics(toAggregate.getId(), toAggregate.getName());

                    newTraversalMetrics.metrics.put(metricsId, aggregateMetrics);
                    // Set the index of the Metrics
                    for (Map.Entry<Integer, String> entry : inTraversalMetrics.indexToLabelMap.entrySet()) {
                        if (metricsId.equals(entry.getValue())) {
                            newTraversalMetrics.indexToLabelMap.put(entry.getKey(), metricsId);
                            break;
                        }
                    }
                }
                aggregateMetrics.aggregate(toAggregate);
            });
        });
        return newTraversalMetrics;
    }

    public void addMetrics(final MutableMetrics newMetrics, final String id, final int index, final boolean isTopLevel, final String profileStepId) {
        if (isTopLevel) {
            // The index is necessary to ensure that step order is preserved after a merge.
            indexToLabelMap.put(index, id);
            metrics.put(id, newMetrics);
        }
        allMetrics.put(profileStepId, newMetrics);
    }
}
