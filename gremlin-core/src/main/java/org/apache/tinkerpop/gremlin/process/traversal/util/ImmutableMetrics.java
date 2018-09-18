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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link Metrics} implementation that cannot be modified.
 *
 * @author Bob Briody (http://bobbriody.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ImmutableMetrics implements Metrics, Serializable {

    static final TimeUnit SOURCE_UNIT = TimeUnit.NANOSECONDS;

    protected String id;
    protected String name;
    protected Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    protected long durationNs = 0l;
    protected final Map<String, Object> annotations = Collections.synchronizedMap(new LinkedHashMap<>());
    protected final Map<String, ImmutableMetrics> nested = new LinkedHashMap<>();

    protected ImmutableMetrics() {
        // necessary for gryo serialization
    }

    @Override
    public long getDuration(final TimeUnit unit) {
        return unit.convert(this.durationNs, SOURCE_UNIT);
    }

    @Override
    public Long getCount(final String key) {
        if (!counts.containsKey(key)) {
            return null;
        }
        return counts.get(key).get();
    }

    @Override
    public Map<String, Long> getCounts() {
        final Map<String, Long> ret = new HashMap<>();
        for (Map.Entry<String, AtomicLong> count : counts.entrySet()) {
            ret.put(count.getKey(), count.getValue().get());
        }
        return ret;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    @Override
    public Collection<ImmutableMetrics> getNested() {
        return nested.values();
    }

    @Override
    public ImmutableMetrics getNested(final String metricsId) {
        return nested.get(metricsId);
    }

    @Override
    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    @Override
    public Object getAnnotation(final String key) {
        return annotations.get(key);
    }

    @Override
    public String toString() {
        return "ImmutableMetrics{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", counts=" + counts +
                ", durationNs=" + durationNs +
                '}';
    }
}
