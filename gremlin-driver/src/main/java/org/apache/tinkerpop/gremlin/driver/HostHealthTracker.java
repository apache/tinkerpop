/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks consecutive failures per {@link Host} and determines when a host should be marked unavailable.
 */
final class HostHealthTracker {

    private final ConcurrentHashMap<Host, AtomicInteger> failureCounts = new ConcurrentHashMap<>();
    private final int threshold;

    HostHealthTracker(final int threshold) {
        if (threshold < 1)
            throw new IllegalArgumentException("threshold must be greater than zero");
        this.threshold = threshold;
    }

    /**
     * Records a failure for the given host. Returns {@code true} if the failure count has reached the threshold
     * and the host should be marked unavailable.
     */
    boolean recordFailure(final Host host) {
        final AtomicInteger count = failureCounts.computeIfAbsent(host, h -> new AtomicInteger(0));
        return count.incrementAndGet() >= threshold;
    }

    /**
     * Records a success for the given host, resetting its failure counter.
     */
    void recordSuccess(final Host host) {
        final AtomicInteger count = failureCounts.get(host);
        if (count != null)
            count.set(0);
    }

    /**
     * Returns {@code true} if the host's failure count is below the threshold.
     */
    boolean isAvailable(final Host host) {
        final AtomicInteger count = failureCounts.get(host);
        return count == null || count.get() < threshold;
    }

    int getThreshold() {
        return threshold;
    }
}
