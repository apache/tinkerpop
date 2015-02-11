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
package org.apache.tinkerpop.gremlin.process.util.metric;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public class MutableMetrics extends ImmutableMetrics implements Cloneable {

    // Note: if you add new members then you probably need to add them to the copy constructor;

    private long tempTime = -1l;

    private MutableMetrics() {
        // necessary for kryo serialization
    }

    public MutableMetrics(final String id, final String name) {
        this.id = id;
        this.name = name;
    }


    public void addNested(MutableMetrics metrics) {
        this.nested.put(metrics.getId(), metrics);
    }

    public void start() {
        if (-1 != this.tempTime) {
            throw new IllegalStateException("Internal Error: Concurrent Metrics start. Stop timer before starting timer.");
        }
        this.tempTime = System.nanoTime();
    }

    public void stop() {
        if (-1 == this.tempTime)
            throw new IllegalStateException("Internal Error: Metrics has not been started. Start timer before stopping timer");
        this.durationNs = this.durationNs + (System.nanoTime() - this.tempTime);
        this.tempTime = -1;
    }

    public void finish(final long count) {
        stop();
        this.count += count;
    }

    public void incrementCount(final long count) {
        this.count += count;
    }

    public void aggregate(MutableMetrics other) {
        this.durationNs += other.durationNs;
        this.count += other.count;

        // Merge annotations. If multiple values for a given key are found then append it to a comma-separated list.
        for (Map.Entry<String, String> p : other.annotations.entrySet()) {
            if (this.annotations.containsKey(p.getKey())) {
                final String existing = this.annotations.get(p.getKey());
                final List<String> existingValues = Arrays.asList(existing.split(","));
                if (!existingValues.contains(p.getValue())) {
                    // New value. Append to comma-separated list.
                    this.annotations.put(p.getKey(), existing + ',' + p.getValue());
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
     * Set an annotation value. Duplicates will be overwritten.
     *
     * @param key
     * @param value
     */
    public void setAnnotation(String key, String value) {
        annotations.put(key, value);
    }

    public void setPercentDuration(final double percentDuration) {
        this.percentDuration = percentDuration;
    }

    public void setDuration(final long duration) {
        this.durationNs = duration;
    }

    @Override
    public MutableMetrics getNested(String metricsId) {
        return (MutableMetrics) nested.get(metricsId);
    }

    public ImmutableMetrics getImmutableClone() {
        final ImmutableMetrics clone = new ImmutableMetrics();
        copyMembers(clone);
        this.nested.values().forEach(nested -> clone.nested.put(nested.id, ((MutableMetrics) nested).getImmutableClone()));
        return clone;
    }

    private void copyMembers(final ImmutableMetrics clone) {
        clone.id = this.id;
        clone.name = this.name;
        clone.count = this.count;
        clone.durationNs = this.durationNs;
        clone.percentDuration = this.percentDuration;
    }

    @Override
    public MutableMetrics clone() {
        final MutableMetrics clone = new MutableMetrics();
        copyMembers(clone);
        this.nested.values().forEach(nested -> clone.nested.put(nested.id, ((MutableMetrics) nested).clone()));
        return clone;
    }

}
