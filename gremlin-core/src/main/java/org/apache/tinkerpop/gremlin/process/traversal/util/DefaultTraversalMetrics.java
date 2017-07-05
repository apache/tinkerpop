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
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DefaultTraversalMetrics implements TraversalMetrics, Serializable {
    /**
     * toString() specific headers
     */
    private static final String[] HEADERS = {"Step", "Count", "Traversers", "Time (ms)", "% Dur"};

    private final Map<String, MutableMetrics> metrics = new HashMap<>();
    private final TreeMap<Integer, String> indexToLabelMap = new TreeMap<>();

    /*
    The following are computed values upon the completion of profiling in order to report the results back to the user
     */
    private long totalStepDuration;
    private Map<String, ImmutableMetrics> computedMetrics;

    public DefaultTraversalMetrics() {
    }

    /**
     * This is only a convenient constructor needed for GraphSON deserialization.
     */
    public DefaultTraversalMetrics(final long totalStepDurationNs, final List<MutableMetrics> metricsMap) {
        this.totalStepDuration = totalStepDurationNs;
        this.computedMetrics = new LinkedHashMap<>(this.metrics.size());
        metricsMap.forEach(metric -> this.computedMetrics.put(metric.getId(), metric.getImmutableClone()));
    }

    @Override
    public long getDuration(final TimeUnit unit) {
        return unit.convert(this.totalStepDuration, MutableMetrics.SOURCE_UNIT);
    }

    @Override
    public Metrics getMetrics(final int index) {
        // adjust index to account for the injected profile steps
        return this.computedMetrics.get(this.indexToLabelMap.get(index));
    }

    @Override
    public Metrics getMetrics(final String id) {
        return this.computedMetrics.get(id);
    }

    @Override
    public Collection<ImmutableMetrics> getMetrics() {
        return this.computedMetrics.values();
    }

    @Override
    public String toString() {
        // Build a pretty table of metrics data.

        // Append headers
        final StringBuilder sb = new StringBuilder("Traversal Metrics\n")
                .append(String.format("%-50s %21s %11s %15s %8s", HEADERS));

        sb.append("\n=============================================================================================================");

        appendMetrics(this.computedMetrics.values(), sb, 0);

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
            final Double percentDur = (Double) m.getAnnotation(PERCENT_DURATION_KEY);

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

            if (percentDur != null) {
                sb.append(String.format(" %8.2f", percentDur));
            }

            // process any annotations
            final Map<String,Object> annotations = m.getAnnotations();
            if (!annotations.isEmpty()) {
                // ignore the PERCENT_DURATION_KEY as that is a TinkerPop annotation that is displayed by default
                annotations.entrySet().stream().filter(kv -> !kv.getKey().equals(PERCENT_DURATION_KEY)).forEach(kv -> {
                    final String prefix = "    \\_";
                    final String separator = "=";
                    final String k = prefix + StringUtils.abbreviate(kv.getKey(), 30);
                    final int valueIndentLen = separator.length() + k.length() + indent;
                    final int leftover = 110 - valueIndentLen;

                    final String[] splitValues = splitOnSize(kv.getValue().toString(), leftover);
                    for (int ix = 0; ix < splitValues.length; ix++) {
                        // the first lines gets the annotation prefix. the rest are indented to the separator
                        if (ix == 0) {
                            sb.append(String.format("%n%s", k + separator + splitValues[ix]));
                        } else {
                            sb.append(String.format("%n%s", padLeft(splitValues[ix], valueIndentLen - 1)));
                        }
                    }
                });
            }

            appendMetrics(m.getNested(), sb, indent + 1);
        }
    }

    private static String[] splitOnSize(final String text, final int size) {
        final String[] ret = new String[(text.length() + size - 1) / size];

        int counter = 0;
        for (int start = 0; start < text.length(); start += size) {
            ret[counter] = text.substring(start, Math.min(text.length(), start + size));
            counter++;
        }

        return ret;
    }

    private static String padLeft(final String text, final int amountToPad) {
        // not sure why this method needed to exist. stupid string format stuff and commons utilities wouldn't
        // work for some reason in the context this method was used above.
        String newText = text;
        for (int ix = 0; ix < amountToPad; ix++) {
            newText = " " + newText;
        }
        return newText;
    }

    private void computeTotals() {
        // Create temp list of ordered metrics
        final List<MutableMetrics> tempMetrics = new ArrayList<>(this.metrics.size());
        for (final String label : this.indexToLabelMap.values()) {
            // The indexToLabelMap is sorted by index (key)
            tempMetrics.add(this.metrics.get(label).clone());
        }

        // Calculate total duration
        this.totalStepDuration = 0;
        tempMetrics.forEach(metric -> this.totalStepDuration += metric.getDuration(MutableMetrics.SOURCE_UNIT));

        // Assign %'s
        tempMetrics.forEach(m -> {
            final double dur = m.getDuration(TimeUnit.NANOSECONDS) * 100.d / this.totalStepDuration;
            m.setAnnotation(PERCENT_DURATION_KEY, dur);
        });

        // Store immutable instances of the calculated metrics
        this.computedMetrics = new LinkedHashMap<>(this.metrics.size());
        tempMetrics.forEach(it -> this.computedMetrics.put(it.getId(), it.getImmutableClone()));
    }

    public static DefaultTraversalMetrics merge(final Iterator<DefaultTraversalMetrics> toMerge) {
        final DefaultTraversalMetrics newTraversalMetrics = new DefaultTraversalMetrics();

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
                    for (final Map.Entry<Integer, String> entry : inTraversalMetrics.indexToLabelMap.entrySet()) {
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

    public void setMetrics(final Traversal.Admin traversal, final boolean onGraphComputer) {
        addTopLevelMetrics(traversal, onGraphComputer);
        handleNestedTraversals(traversal, null, onGraphComputer);
        computeTotals();
    }

    private void addTopLevelMetrics(Traversal.Admin traversal, final boolean onGraphComputer) {
        final List<ProfileStep> profileSteps = TraversalHelper.getStepsOfClass(ProfileStep.class, traversal);
        for (int ii = 0; ii < profileSteps.size(); ii++) {
            // The index is necessary to ensure that step order is preserved after a merge.
            final ProfileStep step = profileSteps.get(ii);
            if (onGraphComputer) {
                final MutableMetrics stepMetrics = traversal.getSideEffects().get(step.getId());
                this.indexToLabelMap.put(ii, stepMetrics.getId());
                this.metrics.put(stepMetrics.getId(), stepMetrics);
            } else {
                final MutableMetrics stepMetrics = step.getMetrics();
                this.indexToLabelMap.put(ii, stepMetrics.getId());
                this.metrics.put(stepMetrics.getId(), stepMetrics);
            }
        }
    }

    private void handleNestedTraversals(final Traversal.Admin traversal, final MutableMetrics parentMetrics, final boolean onGraphComputer) {
        long prevDur = 0;
        for (int i = 0; i < traversal.getSteps().size(); i++) {
            final Step step = (Step) traversal.getSteps().get(i);
            if (!(step instanceof ProfileStep))
                continue;

            final MutableMetrics metrics = onGraphComputer ?
                    traversal.getSideEffects().get(step.getId()) :
                    ((ProfileStep) step).getMetrics();

            if (null != metrics) { // this happens when a particular branch never received a .next() call (the metrics were never initialized)
                if (!onGraphComputer) {
                    // subtract upstream duration.
                    long durBeforeAdjustment = metrics.getDuration(TimeUnit.NANOSECONDS);
                    // adjust duration
                    metrics.setDuration(metrics.getDuration(TimeUnit.NANOSECONDS) - prevDur, TimeUnit.NANOSECONDS);
                    prevDur = durBeforeAdjustment;
                }

                if (parentMetrics != null) {
                    parentMetrics.addNested(metrics);
                }

                if (step.getPreviousStep() instanceof TraversalParent) {
                    for (Traversal.Admin<?, ?> t : ((TraversalParent) step.getPreviousStep()).getLocalChildren()) {
                        handleNestedTraversals(t, metrics, onGraphComputer);
                    }
                    for (Traversal.Admin<?, ?> t : ((TraversalParent) step.getPreviousStep()).getGlobalChildren()) {
                        handleNestedTraversals(t, metrics, onGraphComputer);
                    }
                }
            }
        }
    }
}
