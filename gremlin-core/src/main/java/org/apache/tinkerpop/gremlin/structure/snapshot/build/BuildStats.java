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
package org.apache.tinkerpop.gremlin.structure.snapshot.build;

import java.util.List;
import java.util.Map;

/**
 * What a {@link SnapshotBuilder} measured while building: one entry per phase, and the published size of every
 * segment.
 */
public final class BuildStats {

    /**
     * One build phase.
     *
     * @param name         the phase name
     * @param elapsedNanos time spent in the phase
     * @param diskBytes    bytes on disk under the build and scratch directories when the phase ended
     */
    public record Phase(String name, long elapsedNanos, long diskBytes) {
    }

    private final List<Phase> phases;
    private final Map<String, Long> segmentBytes;

    /**
     * @param phases       phases in execution order
     * @param segmentBytes published size in bytes of each segment, keyed by relative path, in ascending path order
     */
    public BuildStats(final List<Phase> phases, final Map<String, Long> segmentBytes) {
        this.phases = List.copyOf(phases);
        this.segmentBytes = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(segmentBytes));
    }

    public List<Phase> phases() {
        return phases;
    }

    public Map<String, Long> segmentBytes() {
        return segmentBytes;
    }

    /**
     * The largest {@link Phase#diskBytes()} across all phases, or 0 if there were none.
     */
    public long peakDiskBytes() {
        long peak = 0;
        for (final Phase p : phases) peak = Math.max(peak, p.diskBytes());
        return peak;
    }

    /**
     * The sum of all published segment sizes.
     */
    public long totalSegmentBytes() {
        long total = 0;
        for (final long b : segmentBytes.values()) total += b;
        return total;
    }
}
