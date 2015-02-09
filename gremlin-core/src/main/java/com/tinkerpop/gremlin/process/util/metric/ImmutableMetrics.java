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
package com.tinkerpop.gremlin.process.util.metric;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public class ImmutableMetrics implements Metrics, Serializable {

    static final TimeUnit SOURCE_UNIT = TimeUnit.NANOSECONDS;

    protected String id;
    protected String name;
    protected long count;
    protected long durationNs = 0l;
    protected double percentDuration = -1;
    protected final Map<String, String> annotations = new HashMap<>();
    protected final Map<String, ImmutableMetrics> nested = new HashMap<>();

    protected ImmutableMetrics() {
    }

    @Override
    public long getDuration(TimeUnit unit) {
        return unit.convert(this.durationNs, SOURCE_UNIT);
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    @Override
    public double getPercentDuration() {
        return this.percentDuration;
    }

    @Override
    public Collection<ImmutableMetrics> getNested() {
        return nested.values();
    }

    @Override
    public ImmutableMetrics getNested(String metricsId) {
        return nested.get(metricsId);
    }

    @Override
    public Map<String, String> getAnnotations() {
        return annotations;
    }

    @Override
    public String toString() {
        return "Metrics{" +
                "durationNs=" + durationNs +
                ", count=" + count +
                ", name='" + name + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
