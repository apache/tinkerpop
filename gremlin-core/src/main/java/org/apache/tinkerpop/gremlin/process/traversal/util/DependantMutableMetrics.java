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

import java.util.concurrent.TimeUnit;

/**
 * This Metrics class handles a metrics chain in which durations are "double counted" by upstream metrics. Durations are
 * corrected upon retrieval by subtracting upstream durations.
 *
 * @author Bob Briody (http://bobbriody.com)
 */
public class DependantMutableMetrics extends MutableMetrics {
    private long prevDur = 0L;
    private DependantMutableMetrics upStreamMetrics;

    private DependantMutableMetrics() {
        // necessary for gryo serialization
        super();
    }

    public DependantMutableMetrics(final String id, final String name, final DependantMutableMetrics upStreamMetrics) {
        super(id, name);
        this.upStreamMetrics = upStreamMetrics;
    }

    /**
     * Returns the actual duration taken by this Metrics by subtracting the duration taken by the upstream Step, if
     * one exists.
     */
    @Override
    public long getDuration(final TimeUnit unit) {
        if (upStreamMetrics == null){
           return unit.convert(super.durationNs, unit);
        } else {
           // upStreamMetrics exists. Subtract that duration since it is time not spent in this step.
           return unit.convert(super.durationNs - upStreamMetrics.durationNs, unit);
        }
    }

    @Override
    protected void copyMembers(final ImmutableMetrics clone) {
        super.copyMembers(clone);
        clone.durationNs = this.getDuration(TimeUnit.NANOSECONDS);
    }
}
