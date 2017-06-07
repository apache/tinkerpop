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

import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public class MutableMetrics extends ImmutableMetrics implements Cloneable {

    // Note: if you add new members then you probably need to add them to the copy constructor;

    private long tempTime = -1L;

    /**
     * Determines if metrics have been finalized, meaning that no more may be collected.
     */
    private volatile boolean finalized = false;

    protected MutableMetrics() {
        // necessary for gryo serialization
    }

    public MutableMetrics(final String id, final String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * Create a {@code MutableMetrics} from an immutable one.
     */
    public MutableMetrics(final Metrics other) {
        this.id = other.getId();
        this.name = other.getName();
        this.annotations.putAll(other.getAnnotations());
        this.durationNs = other.getDuration(TimeUnit.NANOSECONDS);
        other.getCounts().forEach((key, count) -> this.counts.put(key, new AtomicLong(count)));
        other.getNested().forEach(nested -> this.addNested(new MutableMetrics(nested)));
    }

    public void addNested(final MutableMetrics metrics) {
        if (finalized) throw new IllegalStateException("Metrics have been finalized and cannot be modified");
        this.nested.put(metrics.getId(), metrics);
    }

    public void start() {
        if (finalized) throw new IllegalStateException("Metrics have been finalized and cannot be modified");
        if (-1 != this.tempTime) {
            throw new IllegalStateException("Internal Error: Concurrent Metrics start. Stop timer before starting timer.");
        }
        this.tempTime = System.nanoTime();
    }

    public void stop() {
        if (finalized) throw new IllegalStateException("Metrics have been finalized and cannot be modified");
        if (-1 == this.tempTime)
            throw new IllegalStateException("Internal Error: Metrics has not been started. Start timer before stopping timer");
        this.durationNs = this.durationNs + (System.nanoTime() - this.tempTime);
        this.tempTime = -1;
    }

    public void incrementCount(final String key, final long incr) {
        if (finalized) throw new IllegalStateException("Metrics have been finalized and cannot be modified");
        AtomicLong count = this.counts.get(key);
        if (count == null) {
            count = new AtomicLong();
            this.counts.put(key, count);
        }
        count.addAndGet(incr);
    }

    public void setDuration(final long dur, final TimeUnit unit) {
        if (finalized) throw new IllegalStateException("Metrics have been finalized and cannot be modified");
        this.durationNs = TimeUnit.NANOSECONDS.convert(dur, unit);
    }

    public void setCount(final String key, final long val) {
        if (finalized) throw new IllegalStateException("Metrics have been finalized and cannot be modified");
        this.counts.put(key, new AtomicLong(val));
    }

    public void aggregate(final MutableMetrics other) {
        if (finalized) throw new IllegalStateException("Metrics have been finalized and cannot be modified");
        this.durationNs += other.durationNs;
        for (Map.Entry<String, AtomicLong> otherCount : other.counts.entrySet()) {
            AtomicLong thisCount = this.counts.get(otherCount.getKey());
            if (thisCount == null) {
                thisCount = new AtomicLong(otherCount.getValue().get());
                this.counts.put(otherCount.getKey(), thisCount);
            } else {
                thisCount.addAndGet(otherCount.getValue().get());
            }
        }

        // Merge annotations. If multiple values for a given key are found then append it to a comma-separated list.
        for (Map.Entry<String, Object> p : other.annotations.entrySet()) {
            if (this.annotations.containsKey(p.getKey())) {
                // Strings are concatenated
                Object existingVal = this.annotations.get(p.getKey());
                if (existingVal instanceof String) {
                    final List<String> existingValues = Arrays.asList(existingVal.toString().split(","));
                    if (!existingValues.contains(p.getValue())) {
                        // New value. Append to comma-separated list.
                        this.annotations.put(p.getKey(), existingVal.toString() + ',' + p.getValue());
                    }
                } else {
                    // Numbers are summed
                    final Number existingNum = (Number) existingVal;
                    final Number otherNum = (Number) p.getValue();
                    Number newVal;
                    if (existingNum instanceof Double || existingNum instanceof Float) {
                        newVal = existingNum.doubleValue() + otherNum.doubleValue();
                    } else {
                        newVal = existingNum.longValue() + otherNum.longValue();
                    }
                    this.annotations.put(p.getKey(), newVal);
                }
            } else {
                this.annotations.put(p.getKey(), p.getValue());
            }
        }
        this.annotations.putAll(other.annotations);

        // Merge nested Metrics
        other.nested.values().forEach(nested -> {
            MutableMetrics thisNested = (MutableMetrics) this.nested.get(nested.getId());
            if (thisNested == null) {
                thisNested = new MutableMetrics(nested.getId(), nested.getName());
                this.nested.put(thisNested.getId(), thisNested);
            }
            thisNested.aggregate((MutableMetrics) nested);
        });
    }

    /**
     * Set an annotation value. Support exists for Strings and Numbers only. During a merge, Strings are concatenated
     * into a "," (comma) separated list of distinct values (duplicates are ignored), and Numbers are summed.
     */
    public void setAnnotation(final String key, final Object value) {
        if (finalized) throw new IllegalStateException("Metrics have been finalized and cannot be modified");
        if (!(value instanceof String) && !(value instanceof Number)) {
            throw new IllegalArgumentException("Metrics annotations only support String and Number values.");
        }
        annotations.put(key, value);
    }

    @Override
    public MutableMetrics getNested(final String metricsId) {
        return (MutableMetrics) nested.get(metricsId);
    }

    /**
     * Once these metrics are used in computing the final metrics to report through {@link TraversalMetrics} they
     * should no longer be modified and are thus finalized.
     */
    public boolean isFinalized() {
        return finalized;
    }

    /**
     * Gets a copy of the metrics that is immutable. Once this clone is made, the {@link MutableMetrics} can no
     * longer be modified themselves. This prevents custom steps that implement {@link Profiling} from adding to
     * the metrics after the traversal is complete.
     */
    public synchronized ImmutableMetrics getImmutableClone() {
        finalized = true;
        final ImmutableMetrics clone = new ImmutableMetrics();
        copyMembers(clone);
        this.nested.values().forEach(nested -> clone.nested.put(nested.id, ((MutableMetrics) nested).getImmutableClone()));
        return clone;
    }

    protected void copyMembers(final ImmutableMetrics clone) {
        clone.id = this.id;
        clone.name = this.name;
        // Note: This value is overwritten in the DependantMutableMetrics overridden copyMembers method.
        clone.durationNs = this.durationNs;
        for (Map.Entry<String, AtomicLong> c : this.counts.entrySet()) {
            clone.counts.put(c.getKey(), new AtomicLong(c.getValue().get()));
        }
        for (Map.Entry<String, Object> a : this.annotations.entrySet()) {
            clone.annotations.put(a.getKey(), a.getValue());
        }
    }

    @Override
    public MutableMetrics clone() {
        final MutableMetrics clone = new MutableMetrics();
        copyMembers(clone);
        this.nested.values().forEach(nested -> clone.nested.put(nested.id, ((MutableMetrics) nested).clone()));
        return clone;
    }

    public void finish(final long bulk) {
        stop();
        incrementCount(TraversalMetrics.TRAVERSER_COUNT_ID, 1);
        incrementCount(TraversalMetrics.ELEMENT_COUNT_ID, bulk);
    }
}
