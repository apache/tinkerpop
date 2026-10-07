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
    private final Map<String, Long> timers;
    private final long peakBudgetBytes;

    /**
     * @param phases       phases in execution order
     * @param segmentBytes published size in bytes of each segment, keyed by relative path, in ascending path order
     */
    public BuildStats(final List<Phase> phases, final Map<String, Long> segmentBytes) {
        this(phases, segmentBytes, java.util.Map.of(), 0);
    }

    /**
     * As above, with sub-timers inside phases and the builder's peak budgeted heap.
     *
     * @param timers          nanoseconds spent in parts of a phase, keyed by a name such as {@code edge-scan.resolve}, in
     *                        the order given; they overlap the phases and each other as their names document
     * @param peakBudgetBytes the most heap the builder reserved from its memory budget at one time, 0 when it does not
     *                        account for heap
     */
    public BuildStats(final List<Phase> phases, final Map<String, Long> segmentBytes, final Map<String, Long> timers,
                      final long peakBudgetBytes) {
        this.phases = List.copyOf(phases);
        this.segmentBytes = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(segmentBytes));
        this.timers = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(timers));
        this.peakBudgetBytes = peakBudgetBytes;
    }

    public List<Phase> phases() {
        return phases;
    }

    public Map<String, Long> segmentBytes() {
        return segmentBytes;
    }

    /**
     * Nanoseconds spent in parts of a phase, for builders that measure them: {@code edge-scan.source} is the time spent in
     * the source between edges, {@code edge-scan.callback} the time in the builder's per-edge work, and
     * {@code edge-scan.resolve} the part of that spent resolving endpoints to ordinals. Empty if none were measured.
     */
    public Map<String, Long> timers() {
        return timers;
    }

    /**
     * The most heap the builder reserved from its memory budget at one time, or 0 when the builder does not account for
     * heap.
     */
    public long peakBudgetBytes() {
        return peakBudgetBytes;
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
