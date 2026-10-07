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

import java.util.ArrayList;
import java.util.List;

/**
 * The byte account of the hybrid builder. Structures that can live in heap reserve their bytes here before they
 * allocate them. When a reservation does not fit, the largest {@link Spillable} structure is moved to scratch files,
 * and again until the reservation fits or nothing is left to spill. A budget of zero bytes grants nothing, which makes
 * every structure behave as in the streaming builder.
 * <p/>
 * Not thread-safe; a build is single-threaded.
 */
final class BuildBudget {

    /**
     * A structure that holds budgeted heap and can move its contents to a scratch file.
     */
    interface Spillable {
        /**
         * The budgeted bytes currently held in heap.
         */
        long heldBytes();

        /**
         * Moves the contents to scratch and releases the held bytes to the budget. Does nothing if nothing is held.
         */
        void spill();
    }

    private static final BuildBudget NONE = new BuildBudget(0);

    private final long total;
    private final List<Spillable> spillables = new ArrayList<>();
    private long used;
    private long peak;

    BuildBudget(final long total) {
        this.total = total;
    }

    /**
     * A budget that grants no reservation.
     */
    static BuildBudget none() {
        return NONE;
    }

    long total() {
        return total;
    }

    long used() {
        return used;
    }

    long available() {
        return total - used;
    }

    /**
     * The highest number of bytes that were reserved at one time.
     */
    long peak() {
        return peak;
    }

    void register(final Spillable spillable) {
        if (total > 0) spillables.add(spillable);
    }

    void unregister(final Spillable spillable) {
        spillables.remove(spillable);
    }

    /**
     * Reserves bytes, spilling the largest registered structures while the reservation does not fit.
     *
     * @param bytes     the number of bytes wanted
     * @param requester the structure that asks, if it is spillable; when it is the largest it spills itself and the
     *                  reservation is refused
     * @return whether the bytes are reserved
     */
    boolean reserve(final long bytes, final Spillable requester) {
        if (bytes <= 0) return true;
        if (bytes > total) return false;
        while (total - used < bytes) {
            Spillable victim = null;
            long largest = 0;
            for (final Spillable s : spillables) {
                final long held = s.heldBytes();
                if (held > largest) {
                    largest = held;
                    victim = s;
                }
            }
            if (victim == null) return false;
            victim.spill();
            if (victim == requester) return false;
        }
        used += bytes;
        if (used > peak) peak = used;
        return true;
    }

    boolean reserve(final long bytes) {
        return reserve(bytes, null);
    }

    void release(final long bytes) {
        used -= bytes;
        if (used < 0) throw new IllegalStateException("Released more than reserved");
    }
}
