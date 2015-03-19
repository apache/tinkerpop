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

/**
 * This Metrics class handles a metrics chain in which durations are "double counted" by upstream metrics. Durations are
 * corrected on-the-fly by subtracting upstream durations on every call to stop().
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

    public void start() {
        super.start();
    }

    public void stop() {
        super.stop();
        // root step will not have an upstream metrics
        if (upStreamMetrics != null) {
            // subtract time that is "double counted" by upstream metrics
            super.durationNs -= upStreamMetrics.getAndResetIncrementalDur();
        }
    }

    public long getAndResetIncrementalDur() {
        long incrementalDur = super.durationNs - prevDur;
        prevDur = super.durationNs;
        return incrementalDur;
    }
}
